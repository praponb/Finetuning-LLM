SELECT
    oi.unit_price,
    p.launch_date,
    o.amount,
    o.order_date
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
WHERE p.price < 1160
ORDER BY o.amount DESC
