SELECT is_flagged, MAX(amount) - MIN(amount) AS amount_range FROM transactions WHERE txn_time < '2024-12-01' GROUP BY 1 HAVING MAX(amount) - MIN(amount) > 29;
