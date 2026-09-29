SELECT s.city, COUNT(p.id) AS product_count, AVG(p.stock_qty) AS avg_stock_qty
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
GROUP BY s.city
ORDER BY product_count DESC
