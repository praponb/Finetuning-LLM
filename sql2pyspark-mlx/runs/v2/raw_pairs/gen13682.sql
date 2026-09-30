SELECT
    we.session_id,
    we.page,
    s.started_at,
    CAST(we.duration_sec AS DOUBLE) AS duration_sec_num
FROM web_events we
JOIN sessions s ON we.session_id = s.id
WHERE we.page IS NULL
ORDER BY s.started_at DESC;
