WITH store_totals AS (
    SELECT store_id, COUNT(units) AS units_count, MAX(units) - MIN(units) AS units_range
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.store_name,
    s.region,
    st.units_count,
    st.units_range
FROM stores s
LEFT JOIN store_totals st ON s.id = st.store_id
ORDER BY 2 DESC;
