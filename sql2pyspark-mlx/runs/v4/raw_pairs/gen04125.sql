SELECT a.id, a.opened_date
FROM accounts a
WHERE a.customer_id IN (11, 14) AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.merchant <> 'Lazada')
