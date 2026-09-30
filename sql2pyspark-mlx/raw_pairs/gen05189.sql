SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    p.supplier_id,
    p.category,
    s.rating,
    s.country
FROM order_items oi
RIGHT JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY oi.product_id DESC
