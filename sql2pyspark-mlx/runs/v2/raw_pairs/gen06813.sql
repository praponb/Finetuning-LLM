SELECT status, plan, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range FROM subscriptions GROUP BY status, plan ORDER BY plan, status;
