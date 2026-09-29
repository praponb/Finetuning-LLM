SELECT
    oi.order_id,
    o.shipping_fee,
    o.status,
    ROUND(o.discount, 1) AS discount_rounded
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE oi.quantity BETWEEN 3 AND 5 OR o.shipping_fee BETWEEN 19 AND 33
