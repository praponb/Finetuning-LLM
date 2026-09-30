WITH all_events AS (
    -- Stack the branches
    SELECT course_id, enrolled_at AS event_time, score
    FROM enrollments
    UNION ALL
    SELECT course_id, enrolled_at, score
    FROM enrollments
    WHERE term IS NOT NULL
)
SELECT course_id, COUNT(*) AS n_rows, SUM(score) AS total_score, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY course_id
ORDER BY course_id
