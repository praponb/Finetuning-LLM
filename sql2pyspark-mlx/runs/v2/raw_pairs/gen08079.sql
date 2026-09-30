SELECT plan, SUM(monthly_fee) AS total_monthly_fee, SUM(SUM(monthly_fee)) OVER (ORDER BY plan) AS cumulative_monthly_fee FROM subscriptions WHERE status != 'ACTIVE' GROUP BY plan
