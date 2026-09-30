SELECT
    oi.quantity,
    oi.order_id,
    p.supplier_id,
    s.supplier_name,
    s.rating
FROM order_items oi
FULL OUTER JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
