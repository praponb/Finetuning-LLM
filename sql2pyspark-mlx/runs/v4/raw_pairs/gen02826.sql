SELECT
    oi.order_id,
    oi.quantity,
    p.brand,
    p.product_name,
    s.country,
    s.city,
    CEIL(oi.unit_price) AS unit_price_whole
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE oi.unit_price <= 160 AND p.supplier_id IN (15, 25, 29)
ORDER BY product_name
