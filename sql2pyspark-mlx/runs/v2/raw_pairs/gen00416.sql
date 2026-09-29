SELECT a.id, a.customer_id
FROM accounts a
WHERE a.opened_date BETWEEN '2023-06-14' AND '2024-04-15' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.merchant != 'Amazon')
ORDER BY a.id DESC
