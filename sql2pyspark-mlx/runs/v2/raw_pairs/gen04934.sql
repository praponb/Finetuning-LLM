WITH recent_payments AS (
    SELECT *, CONCAT(method, ' ', status) AS method_status
    FROM payments
    WHERE amount BETWEEN 950 AND 1010
),
payments_stats AS (
    SELECT status, MIN(amount) AS min_amount, SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END) AS cash_count
    FROM recent_payments
    GROUP BY status
)
SELECT *
FROM payments_stats
ORDER BY min_amount
