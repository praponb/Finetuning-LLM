SELECT id, merchant, txn_time FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS row_num FROM transactions) AS r WHERE row_num <= 2 ORDER BY txn_time;
