SELECT
    o.shipping_fee,
    o.store_id,
    o.customer_id,
    oi.order_id
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id
ORDER BY store_id DESC
