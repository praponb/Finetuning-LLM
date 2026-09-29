SELECT plan, monthly_fee_range, basic_count
FROM (
    SELECT plan, MAX(monthly_fee) - MIN(monthly_fee) monthly_fee_range, COUNT(CASE WHEN plan = 'basic' THEN 1 END) basic_count
    FROM subscriptions
    GROUP BY plan
) s
WHERE basic_count >= 7
