WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled,
        LENGTH(TRIM(dept)) AS dept_len,
        CONCAT(level, '#', CAST(id AS STRING)) AS level_ref
    FROM courses
    WHERE course_code IS NOT NULL
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.level,
        s.credits_filled,
        s.dept_len,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id
    FROM cleaned s
    LEFT JOIN enrollments e ON e.course_id = s.id
)
SELECT id, enrollment_id, measure, value
FROM (
    SELECT id, enrollment_id, credits_filled, dept_len
    FROM exploded_detail_enrollments
) src
UNPIVOT (
    value FOR measure IN (credits_filled AS credits_filled_v, dept_len AS m_dept_len)
) unp
