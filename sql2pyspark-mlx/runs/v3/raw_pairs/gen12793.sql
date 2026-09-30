SELECT id, txn_time, ROW_NUMBER() OVER (ORDER BY txn_time, id) rn FROM transactions ORDER BY id DESC;
