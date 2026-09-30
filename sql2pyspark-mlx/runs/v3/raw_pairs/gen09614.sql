WITH recent_payments AS (
    SELECT *
    FROM payments
    WHERE status = 'FAILED'
),
agg AS (
    SELECT HOUR(paid_at) AS paid_hour, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM recent_payments
    GROUP BY HOUR(paid_at)
)
SELECT paid_hour, stddev_amount
FROM agg
ORDER BY stddev_amount DESC, paid_hour
LIMIT 10;
