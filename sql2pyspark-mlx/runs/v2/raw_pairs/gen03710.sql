SELECT monthly_fee, start_date, end_date, COALESCE(monthly_fee, 0) AS monthly_fee_filled FROM subscriptions ORDER BY monthly_fee_filled DESC
