WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY merchant ORDER BY txn_time) AS pos FROM transactions WHERE NOT is_flagged) SELECT * FROM numbered WHERE pos <= 5
