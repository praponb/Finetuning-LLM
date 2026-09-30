SELECT
    oi.id,
    oi.unit_price,
    o.status,
    o.amount,
    p.supplier_id
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
JOIN products p ON oi.product_id = p.id
