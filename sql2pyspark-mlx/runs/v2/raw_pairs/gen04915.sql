SELECT
    t.account_id,
    t.merchant,
    a.account_type,
    c.credit_limit,
    c.name,
    INITCAP(TRIM(t.merchant)) AS merchant_title
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE c.signup_date > '2024-09-01' AND t.is_flagged
ORDER BY merchant_title ASC, t.account_id ASC;
