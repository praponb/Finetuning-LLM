WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE discount <= 0.32 AND status NOT IN ('CANCELLED', 'SHIPPED', 'COMPLETED')
),
StatusSummary AS (
    SELECT status, ROUND((100 * SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_store, MIN(order_date) AS min_order_date
    FROM recent_orders
    GROUP BY status
)
SELECT status, pct_store, min_order_date
FROM StatusSummary
ORDER BY status DESC, pct_store DESC
