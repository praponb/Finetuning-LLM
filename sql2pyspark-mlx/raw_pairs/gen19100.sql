SELECT *, LENGTH(status) AS status_length FROM subscriptions WHERE end_date < '2024-03-01' AND (plan = 'enterprise' OR status = 'PAUSED');
