SELECT status, min_monthly_fee
FROM (
    SELECT status, MIN(monthly_fee) min_monthly_fee
    FROM subscriptions
    GROUP BY status
) s
WHERE min_monthly_fee > 200
ORDER BY min_monthly_fee DESC;
