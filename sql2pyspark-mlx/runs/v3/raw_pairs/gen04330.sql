SELECT
    a.id,
    a.balance,
    a.status,
    a.opened_date,
    c.name,
    c.age,
    c.country
FROM accounts a
INNER JOIN customers c ON c.id = a.customer_id;
