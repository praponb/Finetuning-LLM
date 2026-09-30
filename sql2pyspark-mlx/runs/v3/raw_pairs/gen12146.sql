WITH store_totals AS (
    SELECT store_id, SUM(units) AS total_units
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    s.sqft,
    st.total_units
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
WHERE st.total_units >= 120
ORDER BY region;
