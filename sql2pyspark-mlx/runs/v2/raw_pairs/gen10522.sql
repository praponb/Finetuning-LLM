SELECT id, start_date, monthly_fee FROM subscriptions WHERE plan NOT IN ('basic', 'premium') AND customer_id IS NOT NULL ORDER BY monthly_fee DESC
