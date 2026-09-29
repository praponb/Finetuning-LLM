WITH supplier_totals AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values, MIN(launch_date) AS min_launch_date
    FROM products
    WHERE MONTH(launch_date) IN (2, 5, 11)
    GROUP BY supplier_id
)
SELECT s.city, st.category_values, st.min_launch_date
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
ORDER BY min_launch_date ASC, 1 DESC
