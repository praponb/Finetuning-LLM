SELECT
    p.id,
    p.brand,
    COUNT(oi.id) order_item_count,
    ROUND(AVG(oi.unit_price), 2) avg_unit_price
FROM products p
INNER JOIN order_items oi ON p.id = oi.product_id
WHERE ARRAY_CONTAINS(p.tags, 'sale')
GROUP BY p.id, p.brand
HAVING COUNT(oi.id) >= 4;
