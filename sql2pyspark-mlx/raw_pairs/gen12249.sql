SELECT * FROM (SELECT is_flagged, txn_type, COUNT(amount) amount_count, MIN(amount) min_amount FROM transactions GROUP BY is_flagged, txn_type) t WHERE min_amount < 2300 ORDER BY txn_type ASC
