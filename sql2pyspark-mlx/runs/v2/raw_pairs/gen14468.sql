SELECT
    a.id,
    a.opened_date,
    c.credit_limit,
    c.name
FROM accounts a
JOIN customers c ON a.customer_id = c.id
WHERE a.account_type IN ('SAVINGS', 'CHECKING', 'LOAN') OR a.account_type = 'CREDIT'
ORDER BY c.name, id DESC;
