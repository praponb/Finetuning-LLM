SELECT *
FROM (
    SELECT plan, MIN(monthly_fee) AS min_monthly_fee
    FROM subscriptions
    GROUP BY plan
) AS sub
WHERE min_monthly_fee >= 150
