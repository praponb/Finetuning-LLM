SELECT CASE WHEN status IN ('PAUSED', 'EXPIRED') THEN 'core' ELSE 'other' END AS status_group, MAX(start_date) AS last_start_date FROM subscriptions GROUP BY status_group
