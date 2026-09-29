WITH base_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE (course_code IS NOT NULL OR dept <> 'Bio')
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(score) AS min_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM base_courses s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
per_group AS (
    -- Group-level totals
    SELECT
        credits,
        COUNT(DISTINCT dept) AS distinct_depts,
        SUM(min_enrollment_score) AS total_min_enrollment_score,
        COUNT(*) AS cnt,
        ROUND(SUM(enrollment_count) / NULLIF(SUM(credits), 0), 3) AS enrollment_count_to_credits_ratio
    FROM profiled
    GROUP BY credits
)
-- Native SQL UNPIVOT (wide to long)
SELECT credits, metric_name, metric_value
FROM (
    SELECT credits, total_min_enrollment_score, enrollment_count_to_credits_ratio
    FROM per_group
) src
UNPIVOT (
    metric_value FOR metric_name IN (total_min_enrollment_score AS total_min_enrollment_score_v, enrollment_count_to_credits_ratio AS m_enrollment_count_to_credits_ratio)
) unp
ORDER BY credits, metric_name
