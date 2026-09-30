WITH all_events AS (
    -- Stack the branches
    SELECT grade_status AS label, enrolled_at AS event_time
    FROM enrollments
    UNION ALL
    SELECT term AS label, enrolled_at AS event_time
    FROM enrollments
)
SELECT label, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY label
ORDER BY label
