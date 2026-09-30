SELECT s.country, MAX(p.stock_qty) AS max_stock_qty
FROM products p
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.cost IS NULL
GROUP BY s.country
ORDER BY s.country DESC
