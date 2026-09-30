SELECT id, plan, end_date FROM subscriptions WHERE plan NOT IN ('premium', 'enterprise', 'basic') INTERSECT SELECT id, plan, end_date FROM subscriptions WHERE YEAR(end_date) >= 2024 ORDER BY end_date
