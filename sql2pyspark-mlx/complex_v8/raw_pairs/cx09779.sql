WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len,
        CASE WHEN grade_status IN ('FAIL', 'PASS') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE score BETWEEN 5.6 AND 63.9
      AND grade_status IN ('FAIL', 'WITHDRAWN')
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at,
        grade_status_group,
        CASE WHEN grade_status_group IN ('group_a', 'other') THEN 'group_a' WHEN grade_status_group = 'group_b' THEN 'group_b' ELSE 'other' END AS grade_status_group_group,
        CONCAT_WS(' / ', grade_status, grade_status_group) AS grade_status_grade_status_group_label,
        HOUR(enrolled_at) AS enrolled_at_hour
    FROM prepared
    WHERE grade_status IN ('WITHDRAWN', 'FAIL', 'INCOMPLETE')
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status = 'INCOMPLETE'
              AND e.term IN ('2025T2', '2025T1')
        )
)
-- Final projection
SELECT
    id,
    score,
    enrolled_at,
    grade_status_group_group
FROM screened
WHERE enrolled_at < TIMESTAMP '2025-02-24 10:00:00'
ORDER BY grade_status_group_group DESC, id
LIMIT 20
