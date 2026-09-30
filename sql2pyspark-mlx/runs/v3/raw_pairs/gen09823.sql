SELECT
    o.id,
    o.shipping_fee,
    o.store_id,
    o.status
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.unit_price < 200)
ORDER BY store_id, id DESC
LIMIT 10
