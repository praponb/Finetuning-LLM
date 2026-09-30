WITH top_payments AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY paid_at DESC) AS rn
    FROM payments
)
SELECT *
FROM top_payments
WHERE rn <= 3
ORDER BY paid_at DESC;
