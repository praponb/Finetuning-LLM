SELECT country AS label
FROM sessions
UNION ALL
SELECT status AS label
FROM shipments
