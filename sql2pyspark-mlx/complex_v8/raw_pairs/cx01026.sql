WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE term IN ('2024T2', '2024T1')
)
SELECT student_id, enrolled_at
FROM enrollments
UNION ALL
SELECT course_id AS student_id, enrolled_at
FROM enrollments
WHERE MONTH(enrolled_at) IN (1, 3)
  AND term = '2025T2'
ORDER BY enrolled_at, student_id
