SELECT
    s.customer_id,
    c.email,
    c.segment,
    c.country
FROM subscriptions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id
ORDER BY customer_id DESC;
