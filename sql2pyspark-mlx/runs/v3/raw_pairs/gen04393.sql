WITH valid_payments AS (
    SELECT *, SUBSTRING(method, 1, 4) AS method_prefix
    FROM payments
    WHERE method IN ('card', 'bank_transfer')
),
method_summary AS (
    SELECT
        method,
        TO_DATE(paid_at) AS paid_day,
        AVG(amount) AS avg_amount,
        MAX(amount) - MIN(amount) AS amount_range
    FROM valid_payments
    GROUP BY method, TO_DATE(paid_at)
)
SELECT
    method,
    paid_day,
    avg_amount,
    amount_range
FROM method_summary
ORDER BY amount_range, method, paid_day
LIMIT 5;
