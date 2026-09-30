SELECT status, customer_id FROM subscriptions INTERSECT ALL SELECT status, customer_id FROM subscriptions WHERE monthly_fee > 190;
