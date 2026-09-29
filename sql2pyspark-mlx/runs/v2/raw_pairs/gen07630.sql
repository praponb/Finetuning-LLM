WITH supplier_totals AS (
    SELECT supplier_id, COUNT(DISTINCT price) AS unique_brands, MAX(price) - MIN(price) AS price_range
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    st.unique_brands,
    st.price_range
FROM suppliers s
LEFT OUTER JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.unique_brands >= 9
ORDER BY s.id DESC
