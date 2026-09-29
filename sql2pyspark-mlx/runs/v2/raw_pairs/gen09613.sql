WITH store_totals AS (
    SELECT store_id, MIN(units) AS min_units
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.opened_date,
    s.store_name,
    st.min_units
FROM stores s
LEFT JOIN store_totals st ON s.id = st.store_id
WHERE st.min_units > 31
