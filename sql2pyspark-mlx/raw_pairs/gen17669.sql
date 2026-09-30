WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS latest_rank
    FROM web_events
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    (SELECT AVG(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS avg_web_event_duration_sec
FROM deduped s
ORDER BY session_id DESC;
