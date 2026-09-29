SELECT status FROM accounts WHERE customer_id IN (6, 26, 40) INTERSECT SELECT status FROM subscriptions WHERE start_date > '2023-08-30' ORDER BY status;
