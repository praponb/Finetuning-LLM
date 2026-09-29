SELECT id, account_id, txn_time, amount, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS rn, ROW_NUMBER() OVER (ORDER BY amount, id) AS amount_row FROM transactions
