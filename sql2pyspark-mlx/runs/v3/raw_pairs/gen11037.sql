SELECT status, QUARTER(end_date) AS end_quarter, COUNT(*) AS num_subscriptions FROM subscriptions WHERE status = 'PAUSED' GROUP BY status, QUARTER(end_date) ORDER BY num_subscriptions
