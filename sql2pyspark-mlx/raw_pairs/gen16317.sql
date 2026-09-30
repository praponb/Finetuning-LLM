SELECT
    YEAR(s.started_at) AS started_year,
    s.country,
    COUNT(*) AS row_count,
    MAX(we.duration_sec) AS max_duration_sec
FROM web_events we
JOIN sessions s ON we.session_id = s.id
GROUP BY YEAR(s.started_at), s.country
ORDER BY s.country;
