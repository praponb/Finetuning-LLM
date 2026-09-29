SELECT
    oi.quantity,
    oi.unit_price,
    o.customer_id,
    c.signup_date,
    c.age,
    CONCAT(c.city, '/', c.segment) AS city_segment
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN customers c ON o.customer_id = c.id;
