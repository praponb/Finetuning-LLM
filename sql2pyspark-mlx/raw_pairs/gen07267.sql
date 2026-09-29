WITH supplier_summary AS (
    SELECT supplier_id, SUM(CASE WHEN brand = 'Stark' THEN price ELSE 0 END) AS stark_price, MIN(launch_date) AS first_launch_date
    FROM products
    WHERE price > 770
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.city,
    s.supplier_name,
    ss.stark_price,
    ss.first_launch_date
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
ORDER BY supplier_name, id
LIMIT 5;
