WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE grade_status NOT IN ('FAIL', 'PASS')
      AND term = '2025T1'
),
unioned AS (
    -- Stack the branches
    SELECT id AS key_id, enrolled_at
    FROM enrollments
    WHERE student_id <= 14
    UNION ALL
    SELECT student_id, enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-07 12:00:00'
      AND grade_status = 'WITHDRAWN'
)
SELECT key_id, COUNT(*) AS n_rows, MAX(enrolled_at) AS latest_enrolled_at
FROM unioned
GROUP BY key_id
ORDER BY key_id
