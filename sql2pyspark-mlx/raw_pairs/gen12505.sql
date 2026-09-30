SELECT SUM(CASE WHEN plan = 'premium' THEN 1 ELSE 0 END) AS premium_count, COUNT(monthly_fee) AS monthly_fee_count, SUM(monthly_fee) AS total_monthly_fee FROM subscriptions;
