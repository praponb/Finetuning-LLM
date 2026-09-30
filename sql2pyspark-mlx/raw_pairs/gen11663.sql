SELECT
    t.id,
    t.account_id,
    a.status,
    a.opened_date,
    c.is_active,
    c.country,
    SUBSTRING(t.merchant, 1, 3) AS merchant_prefix
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
FULL OUTER JOIN customers c ON a.customer_id = c.id
WHERE a.balance < 38000 AND t.amount >= 1100
