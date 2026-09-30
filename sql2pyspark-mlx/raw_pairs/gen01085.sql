SELECT 'transaction' AS record_type, amount FROM transactions UNION SELECT 'subscription' AS record_type, monthly_fee FROM subscriptions WHERE plan = 'enterprise' ORDER BY record_type;
