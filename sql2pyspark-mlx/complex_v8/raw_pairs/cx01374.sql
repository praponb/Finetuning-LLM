WITH prepared AS (
    -- Filter courses
    SELECT
        id,
        dept,
        credits,
        level,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown,
        CASE WHEN credits >= 4 THEN 'large' WHEN credits >= 2 THEN 'medium' ELSE 'small' END AS credits_tier,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown_2
    FROM courses
    WHERE level = 'advanced'
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(TRIM(level_or_unknown)) AS level_or_unknown_clean,
        LENGTH(TRIM(level)) AS level_len,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
    FROM prepared
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        COUNT(CASE WHEN grade_status = 'FAIL' THEN 1 END) AS enrollment_grade_status_count,
        MIN(enrolled_at) AS first_enrollment_enrolled_at_2
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count,
        e.first_enrollment_enrolled_at_2
    FROM annotated s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    level_or_unknown,
    credits,
    level_or_unknown_2,
    GROUPING(level_or_unknown) AS is_total_level_or_unknown,
    SUM(enrollment_grade_status_count) AS total_enrollment_grade_status_count,
    MIN(enrollment_grade_status_count) AS min_enrollment_grade_status_count
FROM profiled
GROUP BY GROUPING SETS ((level_or_unknown, credits, level_or_unknown_2), (level_or_unknown), (credits))
ORDER BY level_or_unknown NULLS LAST, credits NULLS LAST, level_or_unknown_2 NULLS LAST
