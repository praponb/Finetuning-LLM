SELECT id, device, DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
FROM sessions
WHERE (started_at < '2023-10-23' AND browser IN ('Firefox', 'Chrome', 'Safari')) OR country IN ('TH', 'JP', 'US')
