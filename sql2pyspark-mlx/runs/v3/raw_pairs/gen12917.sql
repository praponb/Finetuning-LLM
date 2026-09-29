WITH supplier_summary AS (
    SELECT supplier_id, MIN(launch_date) min_launch_date
    FROM products
    WHERE supplier_id = 16
    GROUP BY supplier_id
)
SELECT s.country, ss.min_launch_date
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
ORDER BY min_launch_date;
