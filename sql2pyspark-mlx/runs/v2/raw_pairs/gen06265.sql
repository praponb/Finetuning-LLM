WITH store_totals AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE last_restock <= '2023-12-01'
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    st.quantity_range,
    st.stddev_quantity
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
ORDER BY st.stddev_quantity DESC, id
LIMIT 10;
