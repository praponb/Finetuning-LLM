SELECT id, started_at, UPPER(country) AS country_upper
FROM sessions
WHERE browser IN ('Firefox', 'Chrome', 'Edge') AND (started_at < '2025-03-01' OR country IN ('TH', 'UK'))
ORDER BY started_at;
