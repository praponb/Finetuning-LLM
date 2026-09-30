WITH filtered_orders AS (
    SELECT *
    FROM orders
    WHERE amount > 1210 AND order_date BETWEEN '2023-04-01' AND '2024-03-01'
),
summary AS (
    SELECT DATE_TRUNC('MONTH', order_date) AS order_month, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM filtered_orders
    GROUP BY DATE_TRUNC('MONTH', order_date)
)
SELECT *
FROM summary
