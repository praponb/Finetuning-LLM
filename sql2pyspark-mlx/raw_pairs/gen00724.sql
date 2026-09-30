SELECT
    s.id,
    s.country,
    t.stark_count,
    t.avg_stock_qty
FROM suppliers s
INNER JOIN (SELECT supplier_id, COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count, AVG(stock_qty) AS avg_stock_qty
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
ORDER BY 4, id
LIMIT 5
