SELECT end_date, status FROM subscriptions WHERE customer_id IN (7, 25) INTERSECT SELECT end_date, status FROM subscriptions WHERE start_date <= '2025-10-01';
