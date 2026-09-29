WITH store_totals AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    WHERE product_id IN (7, 8)
    GROUP BY store_id
)
SELECT s.id, s.store_name, st.quantity_range
FROM stores s
LEFT JOIN store_totals st ON s.id = st.store_id
WHERE st.quantity_range > 47
ORDER BY st.quantity_range DESC, s.store_name DESC;
