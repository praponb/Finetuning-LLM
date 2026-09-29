SELECT id, txn_time, amount, txn_type, LEAD(amount, 1) OVER (ORDER BY txn_time, id) next_amount, DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) amount_dense_rank FROM transactions;
