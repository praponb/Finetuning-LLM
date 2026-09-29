WITH supplier_summary AS (
    SELECT supplier_id, MIN(stock_qty) AS min_stock_qty, MIN(launch_date) AS first_launch_date
    FROM products
    WHERE category = 'Grocery'
    GROUP BY supplier_id
)
SELECT s.country, ss.min_stock_qty, ss.first_launch_date
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.min_stock_qty >= 280
