WITH by_dimension AS (
    -- Join a dimension and aggregate
    SELECT
        p.device,
        a.event_type,
        COUNT(*) AS cnt,
        MIN(a.duration_sec) AS min_duration_sec
    FROM web_events a
    INNER JOIN sessions p ON a.session_id = p.id
    GROUP BY GROUPING SETS ((p.device, a.event_type), (p.device))
)
SELECT
    device,
    event_type,
    min_duration_sec
FROM by_dimension
ORDER BY device NULLS LAST;
