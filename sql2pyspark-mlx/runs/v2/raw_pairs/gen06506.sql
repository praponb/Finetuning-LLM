SELECT method, max_amount, last_paid_at
FROM (
    SELECT method, MAX(amount) AS max_amount, MAX(paid_at) AS last_paid_at
    FROM payments
    GROUP BY method
) AS x
WHERE max_amount > 600
