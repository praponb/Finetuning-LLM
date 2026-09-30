WITH scoped_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        dept,
        credits,
        level,
        ABS(credits - 2) AS credits_distance,
        CASE WHEN level IN ('advanced', 'intro') THEN 'group_a' WHEN level = 'core' THEN 'group_b' ELSE 'other' END AS level_group,
        ABS(credits - 3) AS credits_distance_2
    FROM courses
    WHERE (level = 'advanced' OR credits > 2)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(score) AS max_enrollment_score,
        COUNT(*) FILTER (WHERE term = '2025T1') AS enrollment_term_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.max_enrollment_score,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count
    FROM scoped_courses s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM summarized_rows
    WHERE credits_distance > (SELECT MIN(credits_distance) FROM summarized_rows)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM qualified
    WHERE credits_distance_2 > (SELECT MIN(credits_distance_2) FROM qualified)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        credits_distance,
        credits,
        ROUND(SUM(enrollment_term_count) / NULLIF(SUM(credits), 0), 3) AS enrollment_term_count_to_credits_ratio,
        MIN(max_enrollment_score) AS min_max_enrollment_score,
        COUNT(*) AS row_count,
        SUM(enrollment_term_count) FILTER (WHERE level = 'core') AS enrollment_term_count_level_core,
        SUM(CASE WHEN dept = 'Art' THEN 1 ELSE 0 END) AS n_dept_art,
        COUNT(DISTINCT id) AS distinct_ids
    FROM screened
    GROUP BY credits_distance, credits
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(min_max_enrollment_score * 1.5, 2) AS min_max_enrollment_score_adj
    FROM totals
    WHERE enrollment_term_count_to_credits_ratio < 9.0
)
-- Final projection
SELECT
    credits_distance,
    credits,
    row_count,
    distinct_ids,
    min_max_enrollment_score_adj
FROM scored
ORDER BY credits DESC, credits_distance
LIMIT 20
