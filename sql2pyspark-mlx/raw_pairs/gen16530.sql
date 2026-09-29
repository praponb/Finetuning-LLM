WITH top_subscriptions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS row_num
    FROM subscriptions
    WHERE MONTH(start_date) IN (2, 7, 11)
)
SELECT id, plan, monthly_fee
FROM top_subscriptions
WHERE row_num <= 2
ORDER BY plan
