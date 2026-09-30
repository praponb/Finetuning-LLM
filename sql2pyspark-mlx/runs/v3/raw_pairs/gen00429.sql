WITH filtered_orders AS (
    SELECT *, ROUND(discount, 2) AS discount_rounded
    FROM orders
    WHERE discount >= 0.35
),
summary AS (
    SELECT store_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM filtered_orders
    GROUP BY store_id
)
SELECT store_id, status_values
FROM summary
ORDER BY status_values
