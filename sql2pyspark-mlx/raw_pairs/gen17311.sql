SELECT id, txn_type, amount, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS row_num FROM transactions ORDER BY 1 DESC;
