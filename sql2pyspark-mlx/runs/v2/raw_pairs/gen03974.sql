WITH filtered_payments AS (
    SELECT *, INITCAP(TRIM(status)) status_title
    FROM payments
    WHERE paid_at >= '2025-04-16'
),
method_summary AS (
    SELECT method, MIN(amount) min_amount, ROUND((100 * SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_refunded
    FROM filtered_payments
    GROUP BY method
)
SELECT method, min_amount, pct_refunded
FROM method_summary
WHERE min_amount >= 810
ORDER BY min_amount;
