WITH base AS (
    SELECT *
    FROM payments
    WHERE method = 'paypal' AND (order_id = 21 OR amount <= 1210)
),
agg AS (
    SELECT method, COUNT(*) AS row_count
    FROM base
    GROUP BY method
)
SELECT method, row_count
FROM agg
WHERE row_count < 5
