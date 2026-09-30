SELECT
    s.id,
    COUNT(p.id) AS product_count,
    SUM(p.price) AS total_price,
    ROUND(AVG(p.stock_qty), 2) AS avg_stock_qty
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
GROUP BY s.id
ORDER BY s.id DESC
LIMIT 3;
