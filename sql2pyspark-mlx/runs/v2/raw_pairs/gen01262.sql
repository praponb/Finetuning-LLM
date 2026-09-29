SELECT
    a.id,
    a.account_type,
    a.balance,
    c.name,
    c.country,
    c.credit_limit
FROM accounts a
FULL OUTER JOIN customers c ON a.customer_id = c.id
WHERE NOT (a.account_type = 'SAVINGS' AND a.opened_date BETWEEN '2023-10-01' AND '2025-10-01');
