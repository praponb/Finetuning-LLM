SELECT id, account_id, txn_time, amount, LEAD(amount, 2) OVER (PARTITION BY account_id ORDER BY txn_time, id) next_amount FROM transactions ORDER BY account_id DESC, amount;
