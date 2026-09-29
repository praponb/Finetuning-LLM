WITH store_totals AS (
    SELECT store_id, ROUND(AVG(units), 2) AS avg_units, SUM(units) AS total_units
    FROM store_sales
    WHERE units < 21
    GROUP BY store_id
)
SELECT
    s.store_name,
    s.opened_date,
    st.avg_units,
    st.total_units
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
WHERE st.total_units > 200;
