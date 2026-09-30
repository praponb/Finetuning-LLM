SELECT id, amount, account_id, txn_time, ROUND(amount * 0.85, 2) AS amount_adjusted FROM transactions WHERE account_id IN (1, 14) ORDER BY account_id DESC, id ASC LIMIT 3;
