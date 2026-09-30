WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE status NOT IN ('PENDING', 'COMPLETED')
),
Agg AS (
    SELECT
        MONTH(order_date) order_month,
        status,
        COUNT(CASE WHEN channel = 'partner' THEN 1 END) partner_count,
        SUM(amount) total_amount
    FROM recent_orders
    GROUP BY MONTH(order_date), status
)
SELECT *
FROM Agg
ORDER BY 2, order_month
LIMIT 50;
