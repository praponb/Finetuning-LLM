SELECT plan, COUNT(*) total_count, MAX(start_date) last_start_date, MIN(monthly_fee) min_monthly_fee FROM subscriptions WHERE plan <> 'standard' GROUP BY 1 ORDER BY min_monthly_fee DESC, total_count
