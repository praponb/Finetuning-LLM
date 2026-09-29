SELECT * FROM subscriptions WHERE plan IN ('basic', 'standard', 'premium') OR plan = 'standard' ORDER BY id DESC LIMIT 3
