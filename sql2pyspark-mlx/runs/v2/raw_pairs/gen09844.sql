SELECT
    p.supplier_id,
    oi.product_id,
    oi.quantity,
    oi.order_id
FROM products p
JOIN order_items oi ON oi.product_id = p.id
WHERE oi.product_id IS NULL AND oi.quantity > 3
ORDER BY supplier_id DESC, product_id DESC;
