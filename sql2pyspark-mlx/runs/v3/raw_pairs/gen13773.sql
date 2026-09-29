SELECT a.id, a.customer_id
FROM accounts a
WHERE a.opened_date > '2025-03-08' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.merchant != 'Shell');
