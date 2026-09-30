WITH numbered AS (SELECT *, RANK() OVER (PARTITION BY txn_type ORDER BY txn_time DESC) AS rn FROM transactions) SELECT id, txn_type, txn_time FROM numbered WHERE rn <= 5 ORDER BY txn_time
