WITH supplier_totals AS (
    SELECT supplier_id, SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END) AS acme_count, COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count
    FROM products
    WHERE stock_qty > 158
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    st.acme_count,
    st.home_count
FROM suppliers s
LEFT JOIN supplier_totals st ON s.id = st.supplier_id
ORDER BY country DESC;
