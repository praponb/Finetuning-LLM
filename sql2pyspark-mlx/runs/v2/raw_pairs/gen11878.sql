SELECT plan, start_date FROM subscriptions WHERE plan = 'premium' INTERSECT SELECT plan, start_date FROM subscriptions WHERE monthly_fee >= 150 ORDER BY start_date
