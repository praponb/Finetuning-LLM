SELECT
    oi.unit_price,
    oi.quantity,
    p.brand,
    s.country,
    s.supplier_name
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE oi.order_id IN (6, 7) OR oi.unit_price > 170
ORDER BY unit_price DESC;
