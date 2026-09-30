SELECT
    oi.id,
    oi.order_id,
    oi.product_id,
    o.discount,
    o.store_id,
    p.supplier_id
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE p.category IN ('Home', 'Books', 'Electronics') OR o.shipping_fee IS NOT NULL
