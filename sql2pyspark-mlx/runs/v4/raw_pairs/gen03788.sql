SELECT browser, country
FROM sessions
WHERE browser IN ('Safari', 'Edge')
UNION ALL
SELECT browser, country
FROM sessions
WHERE TO_DATE(started_at) < '2025-02-01'
