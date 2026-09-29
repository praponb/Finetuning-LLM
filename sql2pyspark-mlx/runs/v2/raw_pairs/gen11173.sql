SELECT
    a.id,
    a.balance,
    a.account_type,
    c.credit_limit,
    c.age,
    c.city
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
ORDER BY credit_limit DESC, c.city;
