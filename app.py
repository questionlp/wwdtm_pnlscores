# -*- coding: utf-8 -*-
# Copyright (c) 2019 Linh Pham
# wwdtm_winstreaks is relased under the terms of the Apache License 2.0
"""Calculate and display panelist win streaks from scores in the WWDTM
Stats database"""

from collections import OrderedDict
import json
import math
import os
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
import numpy

def retrieve_all_scores(database_connection: mysql.connector.connect
                       ) -> List[int]:
    """Retrieve a list of all panelist scores from non-Best Of and
    non-Repeat shows"""
    cursor = database_connection.cursor()
    query = ("SELECT pm.panelistscore FROM ww_showpnlmap pm "
             "JOIN ww_shows s ON s.showid = pm.showid "
             "WHERE s.bestof = 0 AND s.repeatshowid IS NULL "
             "AND pm.panelistscore IS NOT NULL "
             "ORDER BY pm.panelistscore ASC;")
    cursor.execute(query)
    result = cursor.fetchall()

    if not result:
        return None

    scores = []
    for row in result:
        scores.append(row[0])

    return scores

def retrieve_grouped_scores(database_connection: mysql.connector.connect
                           ) -> Dict:
    """Retrieve a list of grouped panelist scores from non-Best Of and
    non-Repeat shows"""
    cursor = database_connection.cursor()
    query = ("SELECT pm.panelistscore, COUNT(pm.panelistscore) "
             "FROM ww_showpnlmap pm "
             "JOIN ww_shows s ON s.showid = pm.showid "
             "WHERE pm.panelistscore IS NOT NULL "
             "AND s.bestof = 0 AND s.repeatshowid IS NULL "
             "GROUP BY pm.panelistscore "
             "ORDER BY pm.panelistscore ASC;")
    cursor.execute(query)
    result = cursor.fetchall()

    if not result:
        return None

    scores = []
    for row in result:
        score = OrderedDict()
        score["score"] = row[0]
        score["count"] = row[1]
        scores.append(score)

    return scores

def calculate_stats(scores: List[int]):
    """Calculate stats for all of the panelist scores"""
    stats = OrderedDict()
    stats["count"] = len(scores)
    stats["minimum"] = int(numpy.amin(scores))
    stats["maximum"] = int(numpy.amax(scores))
    stats["mean"] = round(numpy.mean(scores), 4)
    stats["median"] = int(numpy.median(scores))
    stats["standard_deviation"] = round(numpy.std(scores), 4)
    stats["total"] = int(numpy.sum(scores))
    return stats

def print_stats(stats: Dict):
    """Print out the score stats"""
    print()
    print("  Panelist Scores")
    print("    Count:      {}".format(stats["count"]))
    print("    Minimum:    {}".format(stats["minimum"]))
    print("    Maximum:    {}".format(stats["maximum"]))
    print("    Median:     {}".format(stats["median"]))
    print("    Mean:       {}".format(stats["mean"]))
    print("    Std Dev:    {}".format(stats["standard_deviation"]))
    print("    Total:      {}".format(stats["total"]))
    print("\n\n")
    return

def print_score_spread(score_spread: Dict):
    """Print out the scrore spread"""
    print("  Score Spread\n")
    print("    Score       Count")
    for score in score_spread:
        print("  {:>7}{:>12}".format(score["score"], score["count"]))

    print()
    return

def load_config(app_environment) -> Dict:
    """Load configuration file from config.json"""
    with open('config.json', 'r') as config_file:
        config_dict = json.load(config_file)

    if app_environment.startswith("develop"):
        if "development" in config_dict:
            config = config_dict["development"]
        else:
            raise Exception("Missing 'development' section in config file")
    elif app_environment.startswith("prod"):
        if "production" in config_dict:
            config = config_dict['production']
        else:
            raise Exception("Missing 'production' section in config file")
    else:
        if "local" in config_dict:
            config = config_dict["local"]
        else:
            raise Exception("Missing 'local' section in config file")

    return config

def main():
    """Pull in scoring data and generate stats based on the data"""
    app_environment = os.getenv("APP_ENV", "local").strip().lower()
    config = load_config(app_environment)
    database_connection = mysql.connector.connect(**config["database"])

    all_scores = retrieve_all_scores(database_connection)
    stats = calculate_stats(all_scores)
    print_stats(stats)

    grouped_scores = retrieve_grouped_scores(database_connection)
    print_score_spread(grouped_scores)

    return None

# Only run if executed as a script and not imported
if __name__ == "__main__":
    main()
