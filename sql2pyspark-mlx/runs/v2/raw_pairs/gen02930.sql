WITH top_payments AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS rnk
    FROM payments
)
SELECT
    id,
    status,
    amount,
    method
FROM top_payments
WHERE rnk <= 2
ORDER BY status ASC
