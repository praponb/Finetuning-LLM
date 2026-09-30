SELECT id, plan, monthly_fee, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS rn FROM subscriptions
