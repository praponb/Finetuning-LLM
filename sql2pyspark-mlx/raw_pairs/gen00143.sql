SELECT
    a.id,
    a.customer_id,
    a.opened_date,
    c.is_active,
    c.city
FROM accounts a
LEFT OUTER JOIN customers c ON a.customer_id = c.id
WHERE a.status = 'OPEN' AND YEAR(a.opened_date) = 2023
