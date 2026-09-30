SELECT *
FROM (
    SELECT status, plan, MIN(monthly_fee) AS min_monthly_fee
    FROM subscriptions
    WHERE monthly_fee < 120
    GROUP BY status, plan
) AS agg
WHERE min_monthly_fee >= 90
ORDER BY min_monthly_fee DESC, status, plan
LIMIT 10
