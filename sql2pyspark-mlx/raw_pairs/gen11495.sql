WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS row_num
    FROM subscriptions
    WHERE status IN ('PAUSED', 'CANCELLED', 'EXPIRED')
)
SELECT *
FROM numbered
WHERE row_num <= 5;
