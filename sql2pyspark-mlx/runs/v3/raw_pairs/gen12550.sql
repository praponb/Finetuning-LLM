WITH supplier_summary AS (
    SELECT supplier_id, MIN(launch_date) AS first_launch_date, SORT_ARRAY(COLLECT_SET(category)) AS category_list
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.city,
    s.country,
    ss.first_launch_date,
    ss.category_list
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
