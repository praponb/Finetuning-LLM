WITH store_totals AS (
    SELECT store_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, ROUND(STDDEV(discount), 2) AS stddev_discount
    FROM orders
    WHERE amount < 810
    GROUP BY store_id
)
SELECT
    s.id,
    s.opened_date,
    st.status_values,
    st.stddev_discount
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id;
