WITH src AS (
    SELECT *
    FROM payments
    WHERE order_id IS NULL
),
payments_stats AS (
    SELECT status, SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) AS promptpay_amount, AVG(amount) AS avg_amount
    FROM src
    GROUP BY status
)
SELECT *
FROM payments_stats
WHERE avg_amount < 800;
