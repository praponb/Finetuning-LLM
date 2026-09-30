SELECT
    s.id,
    s.country,
    COUNT(p.id) AS product_count,
    AVG(p.stock_qty) AS avg_stock_qty,
    ROUND(AVG(p.price), 2) AS avg_price
FROM suppliers s
JOIN products p ON s.id = p.supplier_id
WHERE ARRAY_CONTAINS(p.tags, 'clearance')
GROUP BY s.id, s.country
HAVING avg_price > 68;
