SELECT
    oi.order_id,
    p.supplier_id,
    o.status,
    o.channel
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE o.amount >= 1270 AND o.discount > 0.37
ORDER BY status ASC, supplier_id DESC
