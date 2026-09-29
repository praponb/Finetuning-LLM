WITH store_totals AS (
    SELECT store_id, MIN(order_date) AS min_order_date
    FROM orders
    WHERE discount < 0.38
    GROUP BY store_id
)
SELECT s.id, s.city, st.min_order_date
FROM stores s
LEFT JOIN store_totals st ON s.id = st.store_id
ORDER BY id;
