SELECT id, txn_type, amount, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS amount_row FROM transactions;
