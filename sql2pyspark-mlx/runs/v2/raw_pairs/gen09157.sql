SELECT *
FROM sessions
WHERE NOT (device != 'tablet' AND country IS NULL AND country IN ('US', 'TH'))
ORDER BY browser DESC;
