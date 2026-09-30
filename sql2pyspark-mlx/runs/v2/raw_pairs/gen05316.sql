SELECT
    oi.id,
    oi.quantity,
    oi.order_id,
    p.price,
    s.supplier_name,
    s.rating
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE oi.quantity BETWEEN 6 AND 7 AND s.rating BETWEEN 3.9 AND 4.1;
