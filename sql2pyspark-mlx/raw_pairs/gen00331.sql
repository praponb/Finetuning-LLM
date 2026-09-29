SELECT id, status, end_date FROM subscriptions WHERE monthly_fee IS NOT NULL EXCEPT SELECT id, status, end_date FROM subscriptions WHERE plan IN ('enterprise', 'premium', 'basic')
