SELECT plan, SUM(monthly_fee) AS total_monthly_fee FROM subscriptions GROUP BY plan;
