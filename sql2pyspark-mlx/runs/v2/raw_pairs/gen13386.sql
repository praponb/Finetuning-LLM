SELECT id, txn_type, txn_time, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) AS rn FROM transactions ORDER BY rn, txn_type;
