SELECT * FROM subscriptions WHERE plan IN ('basic', 'standard') AND (plan != 'basic' OR start_date BETWEEN '2025-01-16' AND '2025-02-05') ORDER BY start_date DESC, id LIMIT 50;
