SELECT
    id,
    session_id,
    duration_sec,
    page
FROM (
    SELECT
        id,
        session_id,
        duration_sec,
        page,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS rn
    FROM web_events
) AS ranked
WHERE rn = 1;
