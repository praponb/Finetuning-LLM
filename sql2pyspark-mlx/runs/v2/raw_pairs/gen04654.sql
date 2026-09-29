SELECT
    p.id,
    p.product_name,
    COUNT(oi.id) AS order_item_count,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM products p
LEFT JOIN order_items oi ON oi.product_id = p.id
GROUP BY p.id, p.product_name
