WITH unioned AS (
    -- Stack the branches
    SELECT term AS name, enrolled_at AS event_time, score
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-12 09:00:00'
    UNION ALL
    SELECT term, enrolled_at, score
    FROM enrollments
    UNION ALL
    SELECT grade_status, enrolled_at, score
    FROM enrollments
)
SELECT name, COUNT(*) AS n_rows, SUM(score) AS total_score, MAX(event_time) AS latest_event_time
FROM unioned
GROUP BY name
ORDER BY name
