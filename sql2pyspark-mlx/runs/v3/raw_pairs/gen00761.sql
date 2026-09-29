SELECT
    a.id,
    a.opened_date,
    a.status,
    a.balance
FROM accounts a
WHERE a.customer_id IN (8, 14, 22) AND a.customer_id IN (SELECT id FROM customers WHERE segment = 'SMB')
