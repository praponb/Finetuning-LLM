SELECT txn_type, DATE_TRUNC('MONTH', txn_time) AS txn_month, ROUND(STDDEV(amount), 2) AS stddev_amount FROM transactions GROUP BY txn_type, DATE_TRUNC('MONTH', txn_time) HAVING stddev_amount >= 34;
