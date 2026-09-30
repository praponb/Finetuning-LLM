WITH src AS (SELECT * FROM transactions WHERE txn_type = 'CREDIT'), agg AS (SELECT merchant, AVG(amount) AS avg_amount FROM src GROUP BY merchant) SELECT merchant, avg_amount FROM agg;
