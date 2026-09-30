SELECT txn_type, is_flagged, MAX(amount) AS max_amount, SUM(amount) AS total_amount FROM transactions GROUP BY 1, 2 HAVING MAX(amount) >= 3300 ORDER BY total_amount ASC, max_amount ASC
