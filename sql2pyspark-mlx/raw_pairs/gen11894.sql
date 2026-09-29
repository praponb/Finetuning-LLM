WITH supplier_totals AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
    FROM products
    WHERE price BETWEEN 1040 AND 1200
    GROUP BY supplier_id
)
SELECT
    s.supplier_name,
    s.city,
    st.brand_values,
    st.brand_list
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
ORDER BY st.brand_values DESC
