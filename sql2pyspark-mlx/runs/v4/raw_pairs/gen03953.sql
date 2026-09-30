SELECT browser, DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label
FROM sessions
WHERE HOUR(started_at) BETWEEN 8 AND 16 AND TO_DATE(started_at) < '2025-09-01'
ORDER BY started_at_label;
