WITH filtered_orders AS (
    SELECT *, CONCAT(channel, '/', status) AS channel_status
    FROM orders
    WHERE customer_id IN (19, 35)
),
summary AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values, MIN(order_date) AS min_order_date
    FROM filtered_orders
    GROUP BY customer_id
)
SELECT *
FROM summary
