SELECT id, end_date FROM subscriptions WHERE status = 'PAUSED' INTERSECT SELECT id, end_date FROM subscriptions WHERE end_date < '2025-08-11';
