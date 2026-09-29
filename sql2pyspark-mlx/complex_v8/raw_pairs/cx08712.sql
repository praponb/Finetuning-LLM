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
    WHERE HOUR(enrolled_at) BETWEEN 7 AND 12
      AND term IN ('2025T2', '2024T2')
),
joined_student AS (
    -- Enrich with dimension data
    SELECT s.*, st.enrolled_on AS student_enrolled_on, st.major AS student_major
    FROM filtered s
    JOIN students st ON st.id = s.student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at, grade_status, grade_status_len
    FROM joined_student
) src
PIVOT (
    SUM(grade_status_len)
    FOR grade_status IN ('FAIL', 'PASS', 'INCOMPLETE', 'WITHDRAWN')
)
ORDER BY enrolled_at
