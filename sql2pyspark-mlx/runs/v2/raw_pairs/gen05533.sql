WITH valid_payments AS (
    SELECT *
    FROM payments
    WHERE method IN ('promptpay', 'card') AND status IN ('REFUNDED', 'SUCCESS') AND amount >= 1460
),
agg AS (
    SELECT TO_DATE(paid_at) AS paid_day, AVG(amount) AS avg_amount
    FROM valid_payments
    GROUP BY TO_DATE(paid_at)
)
SELECT paid_day, avg_amount
FROM agg
WHERE avg_amount >= 1030
ORDER BY avg_amount DESC
