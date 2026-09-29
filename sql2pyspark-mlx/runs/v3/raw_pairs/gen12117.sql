SELECT
    t.id,
    t.account_id,
    a.status,
    c.is_active,
    c.email
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
RIGHT JOIN customers c ON a.customer_id = c.id
WHERE t.merchant IN ('Grab', 'Shell')
