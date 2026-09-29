SELECT s.country, SUM(p.stock_qty) AS total_stock_qty
FROM products p
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE s.city IS NOT NULL
GROUP BY s.country
ORDER BY total_stock_qty DESC, country
