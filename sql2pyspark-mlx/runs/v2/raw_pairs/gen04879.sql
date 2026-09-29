WITH base AS (
    SELECT *
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 6 AND 11 AND order_id IS NOT NULL AND order_id = 35
),
payments_stats AS (
    SELECT DATE_FORMAT(paid_at, 'yyyy-MM') paid_month, COUNT(*) cnt, SUM(amount) total_amount
    FROM base
    GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM')
)
SELECT paid_month, total_amount
FROM payments_stats
ORDER BY total_amount DESC;
