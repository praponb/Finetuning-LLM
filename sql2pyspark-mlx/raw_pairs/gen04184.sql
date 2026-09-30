SELECT *
FROM (
    SELECT status, MIN(monthly_fee) AS min_monthly_fee
    FROM subscriptions
    GROUP BY status
) AS agg
WHERE min_monthly_fee < 230
ORDER BY min_monthly_fee DESC
