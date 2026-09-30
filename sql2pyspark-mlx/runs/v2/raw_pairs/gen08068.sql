SELECT DATE_TRUNC('MONTH', s.started_at) AS started_month, ROUND((100 * SUM(CASE WHEN we.event_type = 'search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search
FROM web_events we
JOIN sessions s ON we.session_id = s.id
GROUP BY DATE_TRUNC('MONTH', s.started_at)
ORDER BY pct_search
