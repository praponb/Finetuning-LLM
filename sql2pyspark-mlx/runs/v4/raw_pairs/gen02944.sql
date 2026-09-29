WITH ranked AS (SELECT id, merchant, txn_time, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS pos FROM transactions) SELECT * FROM ranked WHERE pos <= 3 ORDER BY pos DESC
