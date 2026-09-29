SELECT plan, status, COUNT(monthly_fee) AS monthly_fee_count, SUM(monthly_fee) AS total_monthly_fee FROM subscriptions GROUP BY plan, status ORDER BY total_monthly_fee DESC
