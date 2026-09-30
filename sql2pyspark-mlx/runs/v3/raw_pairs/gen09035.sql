WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) AS rnk
    FROM subscriptions
)
SELECT
    id,
    plan,
    monthly_fee,
    start_date
FROM numbered
WHERE rnk = 1;
