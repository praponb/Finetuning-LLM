WITH store_totals AS (
    SELECT store_id, MIN(quantity) AS min_quantity
    FROM inventory
    WHERE store_id IN (5, 6)
    GROUP BY store_id
)
SELECT s.opened_date, st.min_quantity
FROM stores s
LEFT JOIN store_totals st ON s.id = st.store_id;
