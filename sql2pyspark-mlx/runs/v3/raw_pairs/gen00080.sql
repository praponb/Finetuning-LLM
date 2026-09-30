SELECT is_flagged, COUNT(DISTINCT amount) AS unique_txn_types FROM transactions GROUP BY is_flagged HAVING unique_txn_types < 2
