WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
),
all_events AS (
    -- Stack the branches
    SELECT
        'enrollment' AS source,
        grade_status AS category,
        enrolled_at AS event_time
    FROM filtered
    UNION ALL
    SELECT 'enrollment_2' AS source, term, enrolled_at
    FROM enrollments
)
SELECT source, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY source
ORDER BY source
