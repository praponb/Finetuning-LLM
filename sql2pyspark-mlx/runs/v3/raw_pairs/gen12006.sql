SELECT txn_type, avg_amount FROM (SELECT txn_type, AVG(amount) AS avg_amount FROM transactions GROUP BY txn_type) AS agg WHERE avg_amount < 2300;
