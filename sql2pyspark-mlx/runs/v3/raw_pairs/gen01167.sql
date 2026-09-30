WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY monthly_fee) rnk
    FROM subscriptions
)
SELECT *
FROM numbered
WHERE rnk <= 5;
