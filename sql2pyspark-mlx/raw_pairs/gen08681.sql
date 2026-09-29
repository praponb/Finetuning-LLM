SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS pos FROM transactions WHERE amount >= 2600) AS ranked WHERE pos <= 3
