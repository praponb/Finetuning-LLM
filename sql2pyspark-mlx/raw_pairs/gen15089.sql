WITH recent_payments AS (
    SELECT *
    FROM payments
    WHERE amount > 1270
),
agg AS (
    SELECT HOUR(paid_at) AS paid_hour, ROUND(AVG(amount), 2) AS avg_amount, MAX(amount) AS max_amount
    FROM recent_payments
    GROUP BY HOUR(paid_at)
)
SELECT paid_hour, avg_amount, max_amount
FROM agg
ORDER BY avg_amount DESC, paid_hour
LIMIT 10;
