WITH filtered AS (
    SELECT *, CASE WHEN duration_sec >= 473 THEN 'High' WHEN duration_sec >= 234 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier
    FROM web_events
    WHERE page IS NOT NULL
)
SELECT
    src.id,
    src.session_id,
    src.duration_sec,
    d.browser
FROM filtered src
LEFT JOIN sessions d
    ON src.session_id = d.id;
