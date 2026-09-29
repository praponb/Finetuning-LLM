WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) AS rnk
    FROM subscriptions
)
SELECT *
FROM numbered
WHERE rnk <= 3
ORDER BY 2 DESC
