WITH prepared_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CONCAT_WS(': ', course_code, dept) AS course_code_dept_label,
        ABS(credits - 2) AS credits_distance,
        CASE WHEN level IN ('intro', 'advanced') THEN 'group_a' WHEN level = 'core' THEN 'group_b' ELSE 'other' END AS level_group
    FROM courses
    WHERE (LENGTH(TRIM(course_code)) >= 6 OR level = 'intro')
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.level,
        s.level_group,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score
    FROM prepared_courses s
    JOIN enrollments e ON e.course_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    enrollment_id,
    enrollment_student_id,
    level,
    MAX(enrollment_enrolled_at) AS last_enrollment_enrolled_at
FROM exploded_detail_enrollments
GROUP BY ROLLUP(enrollment_id, enrollment_student_id, level)
