WITH valid_orders AS (
    SELECT *
    FROM orders
    WHERE status IN ('COMPLETED', 'SHIPPED')
),
summary AS (
    SELECT status, ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_partner, MAX(order_date) AS last_order_date
    FROM valid_orders
    GROUP BY status
)
SELECT status, pct_partner, last_order_date
FROM summary
WHERE pct_partner < 56
ORDER BY pct_partner, status
