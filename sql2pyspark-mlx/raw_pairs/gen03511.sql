SELECT
    p.id,
    p.category,
    COUNT(oi.id) AS order_item_count,
    ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM products p
LEFT OUTER JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.category
ORDER BY id DESC
