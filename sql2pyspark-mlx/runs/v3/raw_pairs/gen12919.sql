SELECT id, country
FROM sessions
WHERE started_at < '2023-12-01'
UNION
SELECT id, country
FROM sessions
WHERE started_at < '2024-02-23'
