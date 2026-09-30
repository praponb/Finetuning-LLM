SELECT id, end_date, ROW_NUMBER() OVER (ORDER BY end_date DESC, id) rn FROM subscriptions
