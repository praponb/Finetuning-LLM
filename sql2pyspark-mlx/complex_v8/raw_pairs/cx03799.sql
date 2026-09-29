WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE course_id <= 6
),
all_events AS (
    -- Stack the branches
    SELECT
        term AS label,
        enrolled_at AS event_time,
        score AS amount,
        'enrollment' AS source
    FROM cleaned_enrollments
    WHERE student_id >= 14
      AND HOUR(enrolled_at) BETWEEN 8 AND 12
    UNION
    SELECT
        grade_status AS label,
        enrolled_at AS event_time,
        score AS amount,
        'enrollment_2' AS source
    FROM enrollments
)
SELECT source, COUNT(*) AS n_rows, SUM(amount) AS total_amount, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY source
ORDER BY source
