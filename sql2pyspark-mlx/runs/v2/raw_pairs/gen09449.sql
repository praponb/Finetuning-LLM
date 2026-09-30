SELECT
    c.is_active,
    c.segment,
    c.country,
    a.customer_id
FROM customers c
LEFT OUTER JOIN accounts a ON a.customer_id = c.id
WHERE c.credit_limit >= 7300 OR c.email LIKE '%@startup.io'
ORDER BY customer_id DESC, c.segment;
