WITH base AS (
    SELECT *
    FROM payments
    WHERE paid_at >= '2025-05-01' AND amount IS NULL
),
payments_stats AS (
    SELECT status, SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END) AS card_count
    FROM base
    GROUP BY status
)
SELECT *
FROM payments_stats
WHERE card_count > 39;
