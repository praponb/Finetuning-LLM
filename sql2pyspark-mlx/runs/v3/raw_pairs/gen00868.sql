SELECT id, txn_type, amount, RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) amount_rank FROM transactions WHERE amount >= 3000
