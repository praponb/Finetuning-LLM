SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY amount, id) AS row_num FROM transactions QUALIFY row_num = 1;
