SELECT id, merchant, txn_time, is_flagged FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS rn FROM transactions) AS t WHERE rn <= 5 ORDER BY id, merchant
