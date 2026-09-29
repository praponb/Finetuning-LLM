SELECT status, MIN(monthly_fee) min_monthly_fee FROM subscriptions GROUP BY status ORDER BY min_monthly_fee DESC
