SELECT id, txn_type, amount, txn_time, SUM(amount) OVER (PARTITION BY txn_type) AS txn_type_sum_amount, ROW_NUMBER() OVER (ORDER BY txn_time, id) AS rn FROM transactions;
