SELECT id, txn_type, amount, merchant, RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) AS amount_rank, RANK() OVER (PARTITION BY merchant ORDER BY amount) AS rnk FROM transactions;
