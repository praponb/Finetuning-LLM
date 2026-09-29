SELECT
    a.id,
    a.status,
    a.customer_id,
    a.account_type,
    c.name,
    c.city
FROM accounts a
INNER JOIN customers c ON c.id = a.customer_id
ORDER BY account_type DESC
