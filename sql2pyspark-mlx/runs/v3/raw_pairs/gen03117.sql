SELECT launch_date FROM products INTERSECT SELECT end_date FROM subscriptions WHERE start_date BETWEEN '2025-01-01' AND '2025-08-01';
