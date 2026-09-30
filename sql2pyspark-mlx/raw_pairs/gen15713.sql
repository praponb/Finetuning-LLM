SELECT id, monthly_fee, start_date, customer_id FROM subscriptions WHERE monthly_fee > (SELECT MAX(monthly_fee) FROM subscriptions WHERE customer_id = 24) ORDER BY customer_id DESC, id LIMIT 20;
