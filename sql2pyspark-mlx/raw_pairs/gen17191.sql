SELECT monthly_fee, COALESCE(monthly_fee, 0) AS monthly_fee_filled FROM subscriptions;
