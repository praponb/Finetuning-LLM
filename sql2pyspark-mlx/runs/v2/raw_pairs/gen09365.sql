SELECT id, txn_type, amount, RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) AS amount_rank FROM transactions ORDER BY amount DESC;
