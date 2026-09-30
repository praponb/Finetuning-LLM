SELECT id, start_date, status FROM subscriptions WHERE start_date = (SELECT MAX(start_date) FROM subscriptions WHERE plan NOT IN ('standard', 'premium'));
