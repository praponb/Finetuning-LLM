WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, c.level AS course_level
    FROM base s
    JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        SUM(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_sum_1_prior,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at, id), 3) AS enrolled_at_cume_dist,
        DENSE_RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY course_level ORDER BY score NULLS LAST, id) AS score_dense_rank,
        NTH_VALUE(score, 2) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(AVG(score) OVER (PARTITION BY course_level, course_id), 2) AS course_level_course_id_avg_score,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY course_level ORDER BY score NULLS LAST, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        SIZE(COLLECT_SET(student_id) OVER w_course_level) AS distinct_student_id_in_course_level,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_at_ties, 3) AS enrolled_at_cume_dist_2,
        MAX(score) OVER (PARTITION BY course_level) AS course_level_max_score,
        MAX(score) OVER (PARTITION BY course_level, course_id) AS course_level_course_id_max_score
    FROM with_details
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at), w_course_level AS (PARTITION BY course_level)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        course_level,
        enrolled_at_percent_rank,
        score_sum_1_prior,
        enrolled_at_cume_dist,
        enrolled_at_dense_rank,
        second_score,
        course_level_course_id_avg_score,
        minutes_since_prev_enrolled_at,
        distinct_student_id_in_course_level,
        second_enrolled_at,
        score_avg_7,
        enrolled_at_cume_dist_2,
        course_level_max_score,
        course_level_course_id_max_score
    FROM ranked
    WHERE score_dense_rank <= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_grade_status_enrolled_at_den AS rows_in_grade_status_enrolled_at_dense_rank,
        SUM(enrolled_at_cume_dist_2) OVER (PARTITION BY TO_DATE(second_enrolled_at) ORDER BY second_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_enrolled_at_cume_dist_2,
        MAX(score) OVER (PARTITION BY grade_status, enrolled_at_dense_rank) AS grade_status_enrolled_at_dense_rank_max_score,
        LEAD(score_avg_7, 1, 0) OVER (ORDER BY enrolled_at_cume_dist_2 DESC, id) AS next_score_avg_7
    FROM leaders
    WINDOW w_grade_status_enrolled_at_den AS (PARTITION BY grade_status, enrolled_at_dense_rank)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        course_level,
        student_id,
        SUM(CASE WHEN course_level IS NOT NULL THEN 1 ELSE 0 END) AS n_course_level_is_not_null,
        SUM(CASE WHEN course_level NOT IN ('advanced', 'intro') THEN 1 ELSE 0 END) AS n_course_level_not_in_advanced
    FROM windowed
    GROUP BY 1, 2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY course_level) AS rows_in_course_level,
        n_course_level_is_not_null - LAG(n_course_level_is_not_null) OVER (ORDER BY n_course_level_not_in_advanced DESC, course_level, student_id) AS n_course_level_is_not_null_change,
        LAG(n_course_level_is_not_null, 3) OVER (ORDER BY n_course_level_not_in_advanced DESC, course_level, student_id) AS prev3_n_course_level_is_not_null
    FROM summary
)
-- Final projection
SELECT
    course_level,
    student_id,
    n_course_level_is_not_null,
    n_course_level_not_in_advanced,
    rows_in_course_level,
    n_course_level_is_not_null_change,
    prev3_n_course_level_is_not_null,
    CONCAT(course_level, '#', CAST(rows_in_course_level AS STRING)) AS course_level_ref
FROM with_windows
ORDER BY course_level_ref NULLS LAST, n_course_level_not_in_advanced DESC NULLS FIRST
