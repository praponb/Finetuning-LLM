SELECT
    p.id,
    p.brand,
    COUNT(oi.id) AS order_item_count,
    AVG(oi.unit_price) AS avg_unit_price,
    AVG(oi.quantity) AS avg_quantity
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.brand
ORDER BY avg_quantity DESC;
