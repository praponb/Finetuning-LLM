SELECT id, txn_type, amount, txn_time FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) AS row_num FROM transactions) AS t WHERE row_num <= 5 ORDER BY id DESC
