SELECT status FROM payments WHERE status = 'FAILED' UNION SELECT status FROM subscriptions WHERE end_date <= '2024-10-01';
