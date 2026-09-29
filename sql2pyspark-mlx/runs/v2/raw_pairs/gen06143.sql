WITH supplier_totals AS (
    SELECT supplier_id, AVG(price) avg_price
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.rating,
    st.avg_price
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
ORDER BY country DESC, st.avg_price DESC
