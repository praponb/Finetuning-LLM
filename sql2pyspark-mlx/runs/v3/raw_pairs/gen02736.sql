SELECT id, end_date, customer_id FROM subscriptions WHERE end_date < (SELECT MIN(end_date) FROM subscriptions WHERE plan IN ('standard', 'basic')) ORDER BY end_date
