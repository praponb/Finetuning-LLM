WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN level IN ('intro', 'core') THEN 'group_a' WHEN level = 'advanced' THEN 'group_b' ELSE 'other' END AS level_group,
        SUBSTRING(TRIM(course_code), 1, 2) AS course_code_prefix,
        ABS(credits - 2) AS credits_distance
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'C%'
      AND credits <= 3
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.dept,
        s.credits,
        s.level,
        s.level_group,
        s.course_code_prefix,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at
    FROM scoped s
    LEFT JOIN enrollments e ON e.course_id = s.id
)
-- Subtotals with CUBE
SELECT
    enrollment_grade_status,
    level,
    dept,
    GROUPING(enrollment_grade_status) AS is_total_enrollment_grade_status,
    MIN(enrollment_enrolled_at) AS first_enrollment_enrolled_at,
    MIN(credits) AS min_credits
FROM exploded_detail_enrollments
GROUP BY CUBE(enrollment_grade_status, level, dept)
ORDER BY enrollment_grade_status NULLS LAST, level NULLS LAST, dept NULLS LAST
