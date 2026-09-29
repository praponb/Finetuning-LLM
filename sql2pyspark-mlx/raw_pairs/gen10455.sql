WITH supplier_stats AS (
    SELECT supplier_id, ROUND(STDDEV(price), 2) stddev_price
    FROM products
    WHERE brand = 'Initech'
    GROUP BY supplier_id
)
SELECT s.country, ss.stddev_price
FROM suppliers s
INNER JOIN supplier_stats ss ON s.id = ss.supplier_id
ORDER BY stddev_price DESC;
