WITH store_totals AS (
    SELECT store_id, COUNT(DISTINCT amount) AS unique_statuss, MIN(order_date) AS first_order_date
    FROM orders
    WHERE amount > 980
    GROUP BY store_id
)
SELECT
    s.store_name,
    s.sqft,
    st.unique_statuss,
    st.first_order_date
FROM stores s
LEFT JOIN store_totals st ON s.id = st.store_id;
