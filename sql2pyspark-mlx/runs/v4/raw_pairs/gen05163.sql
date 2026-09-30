SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC NULLS FIRST, id) AS rn
FROM transactions
QUALIFY rn = 1;
