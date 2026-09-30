WITH Src AS (
    SELECT *, WEEKOFYEAR(paid_at) AS paid_at_weekofyear
    FROM payments
    WHERE method = 'card'
),
summary AS (
    SELECT
        TO_DATE(paid_at) AS paid_day,
        method,
        MAX(amount) AS max_amount,
        ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cash
    FROM Src
    GROUP BY TO_DATE(paid_at), method
)
SELECT
    paid_day,
    method,
    max_amount,
    pct_cash
FROM summary
WHERE max_amount >= 570
ORDER BY 1, pct_cash DESC
