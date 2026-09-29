WITH valid_payments AS (
    SELECT *
    FROM payments
    WHERE status = 'FAILED'
),
Agg AS (
    SELECT status, COUNT(amount) AS amount_count
    FROM valid_payments
    GROUP BY status
)
SELECT *
FROM Agg
WHERE amount_count >= 3
ORDER BY amount_count DESC, status ASC
LIMIT 5;
