SELECT
    oi.unit_price,
    o.order_date,
    o.amount,
    c.signup_date,
    c.country
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
WHERE oi.product_id IS NOT NULL AND oi.unit_price > 300;
