WITH store_totals AS (
    SELECT store_id, ROUND(AVG(quantity), 2) AS avg_quantity, COUNT(*) AS cnt
    FROM inventory
    WHERE quantity BETWEEN 127 AND 196
    GROUP BY store_id
)
SELECT s.region, st.avg_quantity, st.cnt
FROM stores s
JOIN store_totals st ON s.id = st.store_id
ORDER BY cnt DESC, avg_quantity DESC
