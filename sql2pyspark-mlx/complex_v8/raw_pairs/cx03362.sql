WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
      AND term = '2025T2'
),
unioned AS (
    -- Stack the branches
    SELECT id, grade_status AS category, enrolled_at
    FROM valid_enrollments
    UNION
    SELECT course_id, term, enrolled_at
    FROM enrollments
)
SELECT category, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(enrolled_at) AS latest_enrolled_at
FROM unioned
GROUP BY category
ORDER BY category
