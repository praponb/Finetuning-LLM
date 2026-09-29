SELECT
    s.browser,
    src.event_type,
    COUNT(*) AS rows_total,
    MAX(src.duration_sec) AS max_duration_sec,
    GROUPING_ID() AS grouping_level
FROM web_events src
INNER JOIN sessions s ON src.session_id = s.id
GROUP BY GROUPING SETS ((s.browser, src.event_type), (s.browser), ())
ORDER BY rows_total DESC NULLS FIRST
