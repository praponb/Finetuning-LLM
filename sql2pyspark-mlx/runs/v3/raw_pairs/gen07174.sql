WITH store_totals AS (
    SELECT store_id, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM orders
    WHERE channel IS NULL
    GROUP BY store_id
)
SELECT s.city, st.channel_list, st.status_list
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
ORDER BY city;
