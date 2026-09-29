SELECT status FROM payments WHERE amount BETWEEN 1260 AND 1500 INTERSECT SELECT status FROM subscriptions WHERE end_date <= '2025-05-01';
