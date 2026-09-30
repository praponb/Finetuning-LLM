SELECT txn_type, ROUND((100 * SUM(CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grab FROM transactions GROUP BY txn_type HAVING pct_grab < 68;
