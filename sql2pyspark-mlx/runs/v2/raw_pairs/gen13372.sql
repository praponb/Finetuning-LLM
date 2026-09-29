SELECT id, account_id FROM transactions WHERE account_id IN (12, 15, 20, 36) UNION SELECT id, account_id FROM transactions WHERE merchant IN ('7-Eleven', 'Amazon', 'Starbucks')
