WITH numbered AS (
    SELECT
        id,
        plan,
        monthly_fee,
        status,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee) AS rnk
    FROM subscriptions
)
SELECT
    id,
    plan,
    monthly_fee,
    status
FROM numbered
WHERE rnk <= 3
