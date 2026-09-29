SELECT
    oi.id,
    oi.unit_price,
    p.supplier_id,
    p.category
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
WHERE p.price <= 440;
