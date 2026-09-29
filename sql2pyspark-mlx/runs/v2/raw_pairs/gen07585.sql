WITH store_totals AS (
    SELECT store_id, ROUND(STDDEV(units), 2) AS stddev_units, MAX(sale_date) AS latest_sale_date
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.opened_date,
    s.store_name,
    st.stddev_units,
    st.latest_sale_date
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
WHERE st.stddev_units > 52
ORDER BY st.stddev_units
