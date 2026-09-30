SELECT is_flagged, txn_type, AVG(amount) AS avg_amount FROM transactions GROUP BY 1, 2 HAVING AVG(amount) > 3200 ORDER BY txn_type, avg_amount DESC, is_flagged LIMIT 5;
