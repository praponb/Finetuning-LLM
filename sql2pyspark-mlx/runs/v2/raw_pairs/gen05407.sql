SELECT id, start_date FROM subscriptions WHERE monthly_fee >= 80 INTERSECT SELECT id, start_date FROM subscriptions WHERE plan IN ('enterprise', 'basic');
