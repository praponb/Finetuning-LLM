WITH filtered_payments AS (
    SELECT *, LOWER(method) AS method_lower
    FROM payments
    WHERE status <> 'FAILED'
),
payments_stats AS (
    SELECT method, COUNT(DISTINCT amount) AS unique_statuss
    FROM filtered_payments
    GROUP BY method
)
SELECT method, unique_statuss
FROM payments_stats
ORDER BY unique_statuss DESC;
