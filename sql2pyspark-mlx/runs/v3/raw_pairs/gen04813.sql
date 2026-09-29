SELECT id, txn_type, txn_time FROM (SELECT id, txn_type, txn_time, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS pos FROM transactions) AS r WHERE pos = 1;
