SELECT
    oi.quantity,
    p.cost,
    s.supplier_name,
    s.country
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY supplier_name
