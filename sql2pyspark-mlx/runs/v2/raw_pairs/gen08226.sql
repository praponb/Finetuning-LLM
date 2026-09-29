SELECT
    oi.order_id,
    oi.quantity,
    o.customer_id,
    p.launch_date
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
FULL OUTER JOIN products p ON p.id = oi.product_id;
