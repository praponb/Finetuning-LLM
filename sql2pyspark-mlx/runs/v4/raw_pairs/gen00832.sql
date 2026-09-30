SELECT plan, customer_id FROM subscriptions WHERE status IN ('CANCELLED', 'ACTIVE') UNION SELECT plan, customer_id FROM subscriptions WHERE end_date > '2024-08-01' ORDER BY customer_id;
