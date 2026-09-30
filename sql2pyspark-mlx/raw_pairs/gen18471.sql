WITH filtered_payments AS (
    SELECT *, CAST(amount AS INT) AS amount_int
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 10 AND 13
),
summary AS (
    SELECT HOUR(paid_at) AS paid_hour, ROUND((100 * SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_card
    FROM filtered_payments
    GROUP BY HOUR(paid_at)
)
SELECT paid_hour, pct_card
FROM summary
ORDER BY pct_card DESC, paid_hour;
