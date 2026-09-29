WITH prepared AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CONCAT(course_code, '#', CAST(id AS STRING)) AS course_code_ref,
        ABS(credits - 3) AS credits_distance
    FROM courses
    WHERE level <> 'core'
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.dept,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at
    FROM prepared s
    JOIN enrollments e ON e.course_id = s.id
),
latest_per_key AS (
    -- Keep the earliest row per enrollment_term, dept
    SELECT
        id,
        course_code,
        dept,
        enrollment_id,
        enrollment_term,
        enrollment_student_id,
        enrollment_score,
        enrollment_enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY enrollment_term, dept ORDER BY enrollment_enrolled_at, id, enrollment_id) AS row_num
        FROM line_level
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrollment_student_id, dept, enrollment_score
    FROM latest_per_key
) src
PIVOT (
    MIN(enrollment_score) AS min_v, SUM(enrollment_score) AS sum_v
    FOR dept IN ('Biz', 'Bio', 'Art')
)
ORDER BY enrollment_student_id
