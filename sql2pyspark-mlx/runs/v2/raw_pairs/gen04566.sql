SELECT
    c.country,
    c.age,
    c.name,
    a.balance,
    a.status
FROM customers c
LEFT JOIN accounts a ON c.id = a.customer_id
WHERE c.name NOT LIKE 'He%'
ORDER BY age DESC
