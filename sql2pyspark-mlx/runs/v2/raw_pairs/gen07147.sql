SELECT txn_type, COUNT(*) AS cnt, AVG(amount) AS avg_amount, COUNT(DISTINCT amount) AS unique_merchants FROM transactions GROUP BY txn_type
