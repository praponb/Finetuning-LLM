SELECT
    a.id,
    a.opened_date,
    a.customer_id,
    c.signup_date,
    c.credit_limit
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.status IN ('FROZEN', 'CLOSED', 'OPEN') OR a.opened_date <= '2024-10-01'
ORDER BY customer_id DESC
