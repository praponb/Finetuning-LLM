SELECT
    a.id,
    a.customer_id,
    a.account_type,
    c.credit_limit
FROM accounts a
INNER JOIN customers c ON c.id = a.customer_id
ORDER BY 4 DESC, id
LIMIT 1;
