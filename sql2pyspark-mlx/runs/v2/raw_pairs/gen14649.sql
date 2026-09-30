SELECT
    oi.id,
    oi.order_id,
    p.category,
    p.brand,
    o.store_id,
    o.order_date,
    o.shipping_fee * o.amount AS amount_with_shipping
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price BETWEEN 310 AND 340
ORDER BY store_id DESC, oi.order_id
