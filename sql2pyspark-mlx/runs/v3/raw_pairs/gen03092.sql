WITH supplier_totals AS (
    SELECT supplier_id, COUNT(*) AS product_count, COUNT(DISTINCT stock_qty) AS unique_categorys
    FROM products
    WHERE price BETWEEN 500 AND 1040
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    st.product_count,
    st.unique_categorys
FROM suppliers s
LEFT OUTER JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.unique_categorys > 8
ORDER BY s.id DESC;
