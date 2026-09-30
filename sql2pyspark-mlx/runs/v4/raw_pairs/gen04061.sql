WITH supplier_totals AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.supplier_name,
    st.brand_values,
    st.brand_list
FROM suppliers s
LEFT JOIN supplier_totals st ON s.id = st.supplier_id;
