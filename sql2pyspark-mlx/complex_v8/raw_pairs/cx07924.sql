WITH prepared_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN level IN ('core', 'advanced') THEN 'group_a' WHEN level = 'intro' THEN 'group_b' ELSE 'other' END AS level_group
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'M%'
      AND level IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown,
        CASE WHEN level IN ('advanced', 'core') THEN 'group_a' WHEN level = 'intro' THEN 'group_b' ELSE 'other' END AS level_group_2,
        CASE WHEN level_group IN ('other', 'group_a') THEN 'group_a' WHEN level_group = 'group_b' THEN 'group_b' ELSE 'other' END AS level_group_group
    FROM prepared_courses
),
level_group_2_stats AS (
    -- Avg credits per level_group_2
    SELECT level_group_2, AVG(credits) AS avg_credits
    FROM derived
    GROUP BY level_group_2
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_credits,
        ROUND(s.credits - g.avg_credits, 2) AS credits_minus_avg
    FROM derived s
    JOIN level_group_2_stats g ON g.level_group_2 = s.level_group_2
    WHERE s.credits > g.avg_credits
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(credits_minus_avg) IGNORE NULLS OVER (ORDER BY avg_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credits_minus_avg,
        MIN(credits_minus_avg) OVER (PARTITION BY level_group, dept) AS level_group_dept_min_credits_minus_avg
    FROM compared
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM sequenced s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count,
        MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.min_enrollment_score
    FROM summarized_rows s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.id
)
-- Final projection
SELECT
    id,
    course_code,
    dept,
    level,
    level_or_unknown,
    level_group_2,
    level_group_group,
    avg_credits,
    credits_minus_avg,
    level_group_dept_min_credits_minus_avg,
    last_enrollment_enrolled_at,
    first_enrollment_enrolled_at,
    min_enrollment_score,
    LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug
FROM profiled
ORDER BY course_code_slug, last_enrollment_enrolled_at
