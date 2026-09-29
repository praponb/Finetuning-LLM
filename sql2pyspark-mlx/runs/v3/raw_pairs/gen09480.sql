SELECT a.id, a.account_type, a.status
FROM accounts a
WHERE a.account_type = 'SAVINGS' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.merchant = '7-Eleven')
