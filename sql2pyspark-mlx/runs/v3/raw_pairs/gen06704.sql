SELECT id, plan, monthly_fee, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS row_num, AVG(monthly_fee) OVER (PARTITION BY plan) AS plan_avg_monthly_fee FROM subscriptions
