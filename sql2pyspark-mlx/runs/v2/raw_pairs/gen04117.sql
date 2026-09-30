SELECT account_id, MIN(amount) AS min_amount FROM transactions WHERE is_flagged AND txn_type = 'DEBIT' GROUP BY account_id ORDER BY min_amount;
