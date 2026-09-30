WITH filtered_payments AS (
    SELECT *
    FROM payments
    WHERE method IN ('promptpay', 'bank_transfer', 'card')
),
StatusSummary AS (
    SELECT status, ROUND((100 * SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_card
    FROM filtered_payments
    GROUP BY status
)
SELECT status, pct_card
FROM StatusSummary
ORDER BY pct_card DESC
