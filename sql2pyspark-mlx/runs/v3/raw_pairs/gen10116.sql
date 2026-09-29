WITH store_totals AS (
    SELECT store_id, MAX(order_date) AS latest_order_date, SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count
    FROM orders
    WHERE customer_id IS NOT NULL
    GROUP BY store_id
)
SELECT
    s.city,
    s.store_name,
    st.latest_order_date,
    st.completed_count
FROM stores s
JOIN store_totals st ON s.id = st.store_id
WHERE st.completed_count > 32
ORDER BY completed_count DESC, store_name
