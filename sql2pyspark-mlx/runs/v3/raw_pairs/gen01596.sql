SELECT
    MONTH(s.started_at) AS started_month,
    s.customer_id,
    COUNT(we.duration_sec) AS duration_sec_count,
    AVG(we.duration_sec) AS avg_duration_sec
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
GROUP BY MONTH(s.started_at), s.customer_id
HAVING duration_sec_count >= 1
