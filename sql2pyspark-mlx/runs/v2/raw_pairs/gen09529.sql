WITH supplier_totals AS (
    SELECT supplier_id, SUM(price) AS total_price, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
    FROM products
    WHERE product_name LIKE '%mou%'
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    st.total_price,
    st.category_values
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
