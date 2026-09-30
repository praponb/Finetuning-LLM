WITH supplier_summary AS (
    SELECT supplier_id, MIN(launch_date) AS first_launch_date, COUNT(DISTINCT price) AS unique_suppliers
    FROM products
    WHERE brand IN ('Stark', 'Globex', 'Initech')
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    ss.first_launch_date,
    ss.unique_suppliers
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id;
