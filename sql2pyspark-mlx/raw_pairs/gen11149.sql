SELECT status, customer_id FROM subscriptions INTERSECT ALL SELECT status, customer_id FROM subscriptions WHERE start_date > '2024-05-01'
