WITH supplier_totals AS (
    SELECT supplier_id, SORT_ARRAY(COLLECT_SET(category)) AS category_list, MIN(launch_date) AS min_launch_date
    FROM products
    WHERE price > 640
    GROUP BY supplier_id
)
SELECT
    s.country,
    s.rating,
    st.category_list,
    st.min_launch_date
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
ORDER BY s.rating DESC
