WITH ranked_payments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) AS rnk
    FROM payments
    WHERE amount < 730
)
SELECT
    id,
    method,
    paid_at,
    status
FROM ranked_payments
WHERE rnk <= 3
