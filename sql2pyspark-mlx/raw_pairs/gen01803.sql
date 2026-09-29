SELECT TO_DATE(s.started_at) AS started_day, COUNT(CASE WHEN we.page = '/checkout' THEN 1 END) AS checkout_count, MAX(we.duration_sec) AS max_duration_sec
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
WHERE s.country IN ('JP', 'TH')
GROUP BY TO_DATE(s.started_at)
ORDER BY started_day DESC
