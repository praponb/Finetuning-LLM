SELECT
    a.id,
    a.opened_date,
    a.customer_id,
    a.status
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE country = 'SG');
