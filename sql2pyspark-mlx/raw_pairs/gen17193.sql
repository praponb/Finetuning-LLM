WITH top_payments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS rn
    FROM payments
)
SELECT id, status, paid_at
FROM top_payments
WHERE rn <= 3
ORDER BY 1 DESC;
