SELECT status FROM orders WHERE amount BETWEEN 810 AND 1480 UNION SELECT status FROM subscriptions ORDER BY status DESC;
