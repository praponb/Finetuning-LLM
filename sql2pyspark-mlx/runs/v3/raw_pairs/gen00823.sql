SELECT plan, max_monthly_fee FROM (SELECT plan, MAX(monthly_fee) AS max_monthly_fee FROM subscriptions GROUP BY plan) AS sub WHERE max_monthly_fee < 160 ORDER BY plan DESC LIMIT 1;
