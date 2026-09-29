WITH supplier_stats AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values, MIN(launch_date) AS first_launch_date
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.city,
    ss.category_values,
    ss.first_launch_date
FROM suppliers s
INNER JOIN supplier_stats ss ON s.id = ss.supplier_id;
