SELECT id, monthly_fee FROM subscriptions WHERE customer_id IS NULL AND monthly_fee IS NULL AND customer_id = 27 ORDER BY monthly_fee DESC
