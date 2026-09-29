SELECT txn_type, MAX(amount) - MIN(amount) AS amount_range, ROUND(STDDEV(amount), 2) AS stddev_amount FROM transactions GROUP BY txn_type ORDER BY 1 DESC LIMIT 10;
