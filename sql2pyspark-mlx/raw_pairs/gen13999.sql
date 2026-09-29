WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) AS rn FROM transactions) SELECT id, txn_type, txn_time FROM numbered WHERE rn <= 1
