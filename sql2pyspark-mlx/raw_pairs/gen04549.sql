SELECT end_date, start_date FROM subscriptions WHERE customer_id = 32 INTERSECT SELECT end_date, start_date FROM subscriptions WHERE monthly_fee < 70
