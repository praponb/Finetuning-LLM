SELECT id, start_date, end_date FROM subscriptions WHERE start_date = (SELECT MIN(start_date) FROM subscriptions WHERE plan IN ('basic', 'standard', 'enterprise')) ORDER BY end_date, id LIMIT 1;
