SELECT
    s.id,
    s.shipped_date,
    o.store_id,
    o.customer_id,
    c.email,
    SUBSTRING(c.email, 1, 3) AS email_prefix
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
LEFT JOIN customers c ON o.customer_id = c.id;
