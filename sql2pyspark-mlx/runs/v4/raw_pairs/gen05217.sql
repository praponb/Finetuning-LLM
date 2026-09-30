WITH valid_orders AS (
    SELECT *
    FROM orders
    WHERE discount <= 0.17 OR channel NOT IN ('web', 'store')
),
orders_stats AS (
    SELECT MONTH(order_date) AS order_month, AVG(amount) AS avg_amount, SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END) AS partner_count
    FROM valid_orders
    GROUP BY MONTH(order_date)
)
SELECT *
FROM orders_stats
ORDER BY partner_count DESC, avg_amount DESC
