SELECT COUNT(DISTINCT duration_sec) AS unique_pages
FROM web_events
WHERE duration_sec BETWEEN 160 AND 476
ORDER BY unique_pages DESC
