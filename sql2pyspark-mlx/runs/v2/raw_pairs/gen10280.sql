SELECT country
FROM suppliers
UNION
SELECT country
FROM sessions
WHERE started_at < '2025-10-06';
