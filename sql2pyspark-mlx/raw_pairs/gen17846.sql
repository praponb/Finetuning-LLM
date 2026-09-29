SELECT status, plan, min_monthly_fee
FROM (
    SELECT status, plan, MIN(monthly_fee) AS min_monthly_fee
    FROM subscriptions
    GROUP BY status, plan
) AS agg
WHERE min_monthly_fee >= 70
ORDER BY min_monthly_fee DESC;
