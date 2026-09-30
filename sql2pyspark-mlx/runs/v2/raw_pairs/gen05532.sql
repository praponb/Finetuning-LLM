SELECT
    o.channel,
    oi.quantity,
    oi.unit_price,
    oi.product_id,
    ROUND(o.shipping_fee, 0) AS shipping_fee_rounded
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE oi.unit_price <= 130
