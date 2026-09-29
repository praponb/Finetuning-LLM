SELECT
    oi.quantity,
    oi.order_id,
    p.price,
    p.cost,
    s.city,
    s.supplier_name,
    ROUND(oi.quantity, 1) quantity_rounded
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
