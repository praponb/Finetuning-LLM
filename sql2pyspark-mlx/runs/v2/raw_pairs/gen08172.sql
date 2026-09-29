SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    oi.quantity,
    p.supplier_id
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
