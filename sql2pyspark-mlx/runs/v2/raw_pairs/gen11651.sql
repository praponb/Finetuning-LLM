WITH store_totals AS (
    SELECT store_id, SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END) AS store_count
    FROM orders
    WHERE amount >= 920
    GROUP BY store_id
)
SELECT
    s.id,
    s.sqft,
    s.opened_date,
    st.store_count
FROM stores s
LEFT JOIN store_totals st ON s.id = st.store_id
WHERE st.store_count >= 31
ORDER BY store_count DESC;
