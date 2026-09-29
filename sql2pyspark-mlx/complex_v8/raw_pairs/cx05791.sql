WITH recent AS (
    -- Scope the courses rows
    SELECT
        id,
        dept,
        credits,
        level,
        CASE WHEN dept IN ('Biz', 'CS') THEN 'group_a' WHEN dept = 'Math' THEN 'group_b' ELSE 'other' END AS dept_group
    FROM courses
    WHERE dept IN ('Math', 'Biz')
      AND LOWER(course_code) LIKE '%cs%'
),
labeled AS (
    -- Derive helper columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
    FROM recent
    WHERE dept_group IN ('other', 'group_b')
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.term IS NOT NULL THEN 1 END) FROM enrollments e WHERE e.course_id = s.id) AS enrollment_term_count
    FROM labeled s
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(CASE WHEN grade_status <> 'PASS' THEN 1 END) AS enrollment_grade_status_count,
        COUNT(*) AS enrollment_count,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.first_enrollment_enrolled_at
    FROM with_counts s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrollment_term_count, enrollment_count, dept_group, credits
    FROM profiled
) src
PIVOT (
    COUNT(credits)
    FOR dept_group IN ('other', 'group_a')
)
ORDER BY enrollment_term_count, enrollment_count
