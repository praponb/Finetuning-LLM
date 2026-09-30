WITH filtered_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        CONCAT(level, '#', CAST(id AS STRING)) AS level_ref,
        SUBSTRING(TRIM(level), 1, 3) AS level_prefix,
        ABS(credits - 2) AS credits_distance
    FROM courses
    WHERE level NOT IN ('advanced', 'intro')
      AND credits > 2
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        course_code,
        credits,
        level_ref,
        level_prefix,
        credits - credits_distance AS credits_minus_credits_distance
    FROM filtered_courses
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.first_enrollment_enrolled_at,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM labeled s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
)
SELECT
    enrollment_count,
    credits,
    SUM(credits) AS total_credits
FROM profiled
GROUP BY ROLLUP(enrollment_count, credits)
