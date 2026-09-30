WITH supplier_stats AS (
    SELECT supplier_id, SUM(CASE WHEN category = 'Electronics' THEN stock_qty ELSE 0 END) electronics_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT s.city, s.country, ss.electronics_stock_qty
FROM suppliers s
INNER JOIN supplier_stats ss ON s.id = ss.supplier_id
ORDER BY country
