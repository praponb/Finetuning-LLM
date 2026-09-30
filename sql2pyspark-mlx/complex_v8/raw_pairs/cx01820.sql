WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE term IN ('2024T2', '2024T1')
      AND student_id >= 17
),
unioned AS (
    -- Stack the branches
    SELECT 'enrollment' AS origin, course_id AS qty, enrolled_at AS event_time
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND grade_status <> 'FAIL'
    UNION ALL
    SELECT 'enrollment_2' AS origin, student_id, enrolled_at
    FROM enrollments
)
SELECT origin, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(event_time) AS latest_event_time
FROM unioned
GROUP BY origin
ORDER BY origin
