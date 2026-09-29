SELECT
    a.id,
    a.status,
    a.opened_date,
    a.customer_id,
    c.signup_date,
    c.segment
FROM accounts a
LEFT OUTER JOIN customers c ON a.customer_id = c.id
