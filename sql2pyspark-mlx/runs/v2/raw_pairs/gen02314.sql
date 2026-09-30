SELECT
    oi.id,
    oi.unit_price,
    o.shipping_fee,
    p.cost,
    p.category,
    FLOOR(oi.quantity) AS quantity_whole
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE o.amount <= 900 AND o.channel IS NOT NULL;
