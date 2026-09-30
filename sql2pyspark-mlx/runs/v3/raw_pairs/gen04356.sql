SELECT *
FROM sessions
WHERE browser <> 'Edge' AND (device = 'desktop' OR HOUR(started_at) BETWEEN 10 AND 18)
ORDER BY country
