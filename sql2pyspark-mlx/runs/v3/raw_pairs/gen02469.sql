SELECT
    oi.id,
    oi.product_id,
    p.launch_date,
    p.cost,
    o.amount,
    o.discount / NULLIF(o.shipping_fee, 0) AS discount_per_shipping_fee
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
WHERE o.store_id IN (5, 8, 40)
