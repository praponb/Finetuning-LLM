WITH recent_students AS (
    -- Filter students
    SELECT
        id,
        major,
        enrolled_on,
        is_international,
        UPPER(TRIM(student_name)) AS student_name_clean,
        COALESCE(gpa, 0.0) AS gpa_filled
    FROM students
    WHERE gpa >= 2.7
      AND major = 'CS'
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM recent_students s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.term NOT IN ('2024T2', '2025T1')
              AND e.grade_status = 'FAIL'
        )
)
-- Final projection
SELECT
    id,
    major,
    enrolled_on,
    is_international,
    CASE WHEN major IN ('Math', 'CS') THEN 'group_a' WHEN major = 'Art' THEN 'group_b' ELSE 'other' END AS major_group
FROM qualified
WHERE gpa_filled <= 3.2
ORDER BY major_group DESC, id DESC NULLS FIRST
