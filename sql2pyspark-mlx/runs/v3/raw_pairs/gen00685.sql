SELECT
    a.id,
    a.status,
    a.account_type,
    a.balance,
    c.is_active,
    c.age,
    c.city
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.opened_date <= '2023-07-01' AND c.city IN ('Chiang Mai', 'London', 'Berlin');
