WITH supplier_totals AS (
    SELECT supplier_id, COUNT(DISTINCT cost) AS unique_categorys
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.supplier_name, st.unique_categorys
FROM suppliers s
LEFT JOIN supplier_totals st ON s.id = st.supplier_id
ORDER BY s.id ASC, st.unique_categorys DESC
