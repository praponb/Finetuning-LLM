SELECT
    s.id,
    s.rating,
    s.supplier_name,
    t.avg_price,
    t.pct_globex
FROM suppliers s
LEFT JOIN (SELECT supplier_id, ROUND(AVG(price), 2) AS avg_price, ROUND((100 * SUM(CASE WHEN brand = 'Globex' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_globex
    FROM products
    WHERE stock_qty < 219
    GROUP BY supplier_id) t ON s.id = t.supplier_id
ORDER BY rating DESC
