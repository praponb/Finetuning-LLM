SELECT id, end_date, start_date, customer_id FROM subscriptions WHERE NOT (MONTH(start_date) IN (1, 4, 6) AND end_date < '2025-06-01');
