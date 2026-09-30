SELECT *
FROM (
    SELECT plan, COUNT(CASE WHEN plan = 'standard' THEN 1 END) AS standard_count, COUNT(monthly_fee) AS monthly_fee_count
    FROM subscriptions
    WHERE status NOT IN ('PAUSED', 'ACTIVE')
    GROUP BY plan
) AS agg
WHERE standard_count >= 5
ORDER BY 3;
