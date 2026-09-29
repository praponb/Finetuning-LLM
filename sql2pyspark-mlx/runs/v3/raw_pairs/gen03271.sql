WITH valid_payments AS (
    SELECT *
    FROM payments
    WHERE order_id IS NULL AND (paid_at >= '2025-01-01' OR amount BETWEEN 720 AND 1240)
),
summary AS (
    SELECT status, COUNT(amount) AS amount_count, ROUND((100 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_failed
    FROM valid_payments
    GROUP BY status
)
SELECT status, pct_failed
FROM summary
WHERE pct_failed < 58
ORDER BY status, pct_failed DESC
LIMIT 1;
