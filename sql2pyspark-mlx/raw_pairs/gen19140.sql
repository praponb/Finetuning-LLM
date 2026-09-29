SELECT
    c.name,
    c.email,
    a.opened_date,
    a.customer_id,
    a.status
FROM customers c
JOIN accounts a ON a.customer_id = c.id
