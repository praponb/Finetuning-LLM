WITH valid_orders AS (
    SELECT *
    FROM orders
    WHERE amount > 470
),
status_summary AS (
    SELECT status, MAX(order_date) AS latest_order_date
    FROM valid_orders
    GROUP BY status
)
SELECT *
FROM status_summary
ORDER BY latest_order_date, status DESC
