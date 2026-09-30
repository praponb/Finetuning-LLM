SELECT
    a.id,
    a.status,
    a.opened_date,
    a.customer_id,
    c.name,
    c.email,
    c.age
FROM accounts a
JOIN customers c ON c.id = a.customer_id
WHERE a.status IN ('OPEN', 'FROZEN') AND a.balance > 65000
ORDER BY name;
