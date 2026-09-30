SELECT
    oi.order_id,
    p.cost,
    p.product_name,
    s.country,
    s.rating
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
ORDER BY product_name;
