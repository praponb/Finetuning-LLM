SELECT account_id, txn_time, amount, CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END AS is_starbucks FROM transactions WHERE merchant IN ('Shell', 'Grab', 'Amazon');
