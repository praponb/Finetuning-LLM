SELECT
    a.id,
    a.opened_date,
    a.status,
    a.account_type
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE city IN ('Berlin', 'Singapore', 'New York'))
