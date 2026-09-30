WITH filtered_payments AS (
    SELECT *
    FROM payments
    WHERE method = 'cash'
),
summary AS (
    SELECT method, COUNT(amount) AS amount_count
    FROM filtered_payments
    GROUP BY method
)
SELECT method, amount_count
FROM summary
ORDER BY amount_count DESC, method;
