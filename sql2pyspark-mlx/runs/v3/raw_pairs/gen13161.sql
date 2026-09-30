SELECT end_date FROM subscriptions WHERE plan = 'standard' EXCEPT SELECT shipped_date FROM shipments ORDER BY end_date
