SELECT c.segment, SUM(CASE WHEN s.browser = 'Firefox' THEN 1 ELSE 0 END) AS firefox_count, MIN(s.started_at) AS min_started_at
FROM sessions s
JOIN customers c ON c.id = s.customer_id
WHERE s.device = 'tablet'
GROUP BY c.segment
ORDER BY 1
LIMIT 5;
