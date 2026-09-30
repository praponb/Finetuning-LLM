WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        level,
        LENGTH(TRIM(level)) AS level_len
    FROM courses
    WHERE LENGTH(TRIM(course_code)) >= 6
      AND credits <= 3
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.id) AS first_enrollment_enrolled_at,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.course_id = s.id) AS min_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.id) AS enrollment_count
    FROM base s
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        course_code,
        level_len,
        min_enrollment_score,
        enrollment_count,
        SUBSTRING(TRIM(dept), 1, 2) AS dept_prefix
    FROM profiled
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(enrollment_count) OVER (PARTITION BY enrollment_count ORDER BY enrollment_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_enrollment_count_so_far,
        ROW_NUMBER() OVER w_enrollment_count_by_enrollme_2 AS enrollment_count_seq
    FROM enriched
    WINDOW w_enrollment_count_by_enrollme_2 AS (PARTITION BY enrollment_count ORDER BY enrollment_count DESC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        level_len,
        dept_prefix,
        min_enrollment_count_so_far,
        LEAST(level_len, enrollment_count) AS least_level_len_enrollment_count
    FROM metrics
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(level_len) OVER (PARTITION BY level_len ORDER BY min_enrollment_count_so_far, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS level_len_avg_2,
        NTILE(5) OVER (PARTITION BY level_len ORDER BY min_enrollment_count_so_far, id) AS min_enrollment_count_so_far_quintile
    FROM annotated
)
-- Final projection
SELECT
    id,
    level_len,
    min_enrollment_count_so_far,
    level_len_avg_2,
    ROUND(level_len_avg_2 * 1.07, 2) AS level_len_avg_2_adj,
    SUBSTRING(TRIM(dept_prefix), 1, 3) AS dept_prefix_prefix
FROM metrics_2
ORDER BY min_enrollment_count_so_far DESC
