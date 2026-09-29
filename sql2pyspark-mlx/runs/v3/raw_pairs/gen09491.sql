SELECT
    oi.id,
    oi.quantity,
    p.supplier_id,
    o.channel,
    o.order_date
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
FULL OUTER JOIN orders o ON oi.order_id = o.id
