SELECT
    oi.id,
    oi.order_id,
    oi.product_id,
    p.price,
    p.supplier_id,
    o.customer_id,
    o.order_date
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price <= 350
ORDER BY order_id ASC, id ASC
LIMIT 5
