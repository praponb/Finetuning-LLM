WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE status = 'PAUSED'
),
summary AS (
    SELECT plan, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM src
    GROUP BY plan
)
SELECT *
FROM summary
