WITH store_totals AS (
    SELECT store_id, COUNT(DISTINCT units) unique_products
    FROM store_sales
    WHERE sale_date < '2023-08-11'
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.opened_date,
    st.unique_products
FROM stores s
JOIN store_totals st ON s.id = st.store_id
WHERE st.unique_products >= 4
ORDER BY s.id DESC;
