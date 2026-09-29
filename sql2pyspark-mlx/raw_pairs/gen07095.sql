SELECT plan, SUM(monthly_fee) total_monthly_fee FROM subscriptions WHERE plan IN ('premium', 'enterprise', 'basic') GROUP BY plan HAVING SUM(monthly_fee) < 440;
