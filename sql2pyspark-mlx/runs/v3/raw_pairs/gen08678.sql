SELECT status FROM payments WHERE order_id IS NOT NULL INTERSECT SELECT status FROM subscriptions WHERE start_date <= '2023-03-01';
