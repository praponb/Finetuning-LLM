SELECT
    oi.id,
    oi.order_id,
    o.shipping_fee,
    p.launch_date
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
JOIN products p ON oi.product_id = p.id
WHERE p.launch_date > '2023-11-01'
ORDER BY oi.id
