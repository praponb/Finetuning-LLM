WITH filtered_orders AS (
    SELECT *
    FROM orders
    WHERE amount > 1210 AND order_date BETWEEN '2023-04-01' AND '2024-03-01'
),
summary AS (
    SELECT store_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM filtered_orders
    GROUP BY store_id
)
SELECT *
FROM summary
