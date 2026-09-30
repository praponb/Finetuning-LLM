SELECT id, txn_type, txn_time, merchant FROM (SELECT id, txn_type, txn_time, merchant, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS rnk FROM transactions) AS r WHERE rnk = 1;
