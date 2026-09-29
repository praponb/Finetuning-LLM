SELECT
    a.account_type,
    a.balance,
    a.status,
    c.country
FROM accounts a
FULL OUTER JOIN customers c ON a.customer_id = c.id
ORDER BY c.country ASC;
