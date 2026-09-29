WITH recent_payments AS (
    SELECT *, CASE WHEN status IN ('SUCCESS', 'FAILED') THEN 'Core' ELSE 'Other' END AS status_group
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 11 AND 18
),
method_summary AS (
    SELECT method, COUNT(amount) AS amount_count, SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_count
    FROM recent_payments
    GROUP BY method
)
SELECT method, amount_count, refunded_count
FROM method_summary
WHERE refunded_count >= 28
ORDER BY refunded_count;
