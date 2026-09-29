WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT_WS('-', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE grade_status IN ('WITHDRAWN', 'FAIL')
      AND course_id IS NOT NULL
),
all_events AS (
    -- Stack the branches
    SELECT
        'enrollment' AS origin,
        student_id,
        enrolled_at,
        grade_status
    FROM enrollments
    WHERE term = '2025T2'
      AND TO_DATE(enrolled_at) < DATE '2025-02-24'
    UNION ALL
    SELECT
        'enrollment_2' AS origin,
        student_id,
        enrolled_at,
        grade_status
    FROM enrollments
)
SELECT origin, COUNT(*) AS n_rows, SUM(student_id) AS total_student_id, MAX(enrolled_at) AS latest_enrolled_at
FROM all_events
GROUP BY origin
ORDER BY origin
