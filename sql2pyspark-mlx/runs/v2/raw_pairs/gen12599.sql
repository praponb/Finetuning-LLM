SELECT amount, CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END AS is_grab FROM transactions WHERE account_id = 2 AND merchant IN ('7-Eleven', 'Grab', 'Amazon')
