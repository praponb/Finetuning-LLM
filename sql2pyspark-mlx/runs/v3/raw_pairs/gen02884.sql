SELECT
    oi.order_id,
    oi.quantity,
    p.category,
    p.price,
    p.supplier_id
FROM order_items oi
JOIN products p ON p.id = oi.product_id;
