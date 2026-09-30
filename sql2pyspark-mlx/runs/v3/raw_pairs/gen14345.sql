SELECT CASE WHEN status IN ('PAUSED', 'ACTIVE') THEN 'tier_1' ELSE 'other' END AS status_group, QUARTER(start_date) AS start_quarter, SUM(CASE WHEN plan = 'basic' THEN 1 ELSE 0 END) AS basic_count
FROM subscriptions
GROUP BY 1, 2
HAVING basic_count < 77;
