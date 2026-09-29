SELECT id, txn_time, ROW_NUMBER() OVER (ORDER BY txn_time, id) row_num FROM transactions
