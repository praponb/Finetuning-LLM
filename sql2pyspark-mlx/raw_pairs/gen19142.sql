WITH filtered_orders AS (
    SELECT *
    FROM orders
    WHERE customer_id IN (15, 31, 34, 37)
),
summary AS (
    SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM filtered_orders
    GROUP BY status
)
SELECT status, status_values
FROM summary
ORDER BY status DESC, status_values;
