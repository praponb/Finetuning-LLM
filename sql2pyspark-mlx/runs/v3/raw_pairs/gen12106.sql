SELECT amount, account_id, CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END is_amazon, CAST(is_flagged AS INT) is_flagged_flag FROM transactions WHERE amount <= 3100 ORDER BY account_id;
