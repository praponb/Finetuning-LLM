SELECT end_date, status, CASE WHEN status IN ('EXPIRED', 'PAUSED') THEN 'Core' ELSE 'Other' END status_group FROM subscriptions WHERE end_date <= '2024-03-01';
