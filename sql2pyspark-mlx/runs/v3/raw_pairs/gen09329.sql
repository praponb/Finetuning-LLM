WITH ranked_payments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS pos
    FROM payments
)
SELECT *
FROM ranked_payments
WHERE pos <= 3
ORDER BY pos ASC, paid_at ASC;
