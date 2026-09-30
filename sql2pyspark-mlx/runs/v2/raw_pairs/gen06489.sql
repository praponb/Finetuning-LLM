SELECT plan, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range FROM subscriptions GROUP BY plan ORDER BY plan ASC
