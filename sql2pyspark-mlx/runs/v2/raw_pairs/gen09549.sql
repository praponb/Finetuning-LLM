SELECT
    oi.id,
    oi.product_id,
    o.order_date,
    o.status,
    o.customer_id
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price BETWEEN 230 AND 300;
