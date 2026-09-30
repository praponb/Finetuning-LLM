SELECT
    o.channel,
    o.order_date,
    s.sqft,
    s.city,
    c.signup_date,
    c.segment
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
JOIN customers c ON o.customer_id = c.id
