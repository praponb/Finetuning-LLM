WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE student_id <= 9
      AND grade_status = 'INCOMPLETE'
)
SELECT id AS key_id, enrolled_at
FROM valid_enrollments
WHERE student_id IS NOT NULL
  AND HOUR(enrolled_at) BETWEEN 9 AND 15
UNION ALL
SELECT course_id, enrolled_at
FROM enrollments
UNION ALL
SELECT student_id, enrolled_at
FROM enrollments
WHERE term IN ('2025T2', '2025T1', '2024T1')
  AND grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'PASS')
ORDER BY key_id, enrolled_at
