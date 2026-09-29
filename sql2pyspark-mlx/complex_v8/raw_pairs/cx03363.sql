WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE course_id <= 5
),
stats AS (
    -- Summarise per group
    SELECT
        grade_status,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_ym,
        ROUND(SUM(score), 2) AS total_score,
        COUNT(*) AS row_count,
        MAX(enrolled_at) AS last_enrolled_at,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        COUNT(*) FILTER (WHERE grade_status IS NOT NULL) AS n_grade_status_is_not_null,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM base
    GROUP BY grade_status, DATE_FORMAT(enrolled_at, 'yyyy-MM')
    HAVING SUM(score) > 34.2
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(distinct_course_ids) OVER (PARTITION BY grade_status, distinct_grade_status) AS grade_status_distinct_grade_status_max_distinct_,
        SUM(distinct_grade_status) OVER (ORDER BY n_grade_status_is_not_null, grade_status, enrolled_at_ym ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS distinct_grade_status_sum_6_prior,
        SUM(total_score) OVER (ORDER BY n_grade_status_is_not_null, grade_status, enrolled_at_ym ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS total_score_sum_3_prior,
        RANK() OVER (ORDER BY n_grade_status_is_not_null) AS n_grade_status_is_not_null_rank
    FROM stats
)
-- Final projection
SELECT
    grade_status,
    total_score,
    distinct_course_ids,
    n_grade_status_is_not_null,
    distinct_grade_status,
    grade_status_distinct_grade_status_max_distinct_,
    distinct_grade_status_sum_6_prior,
    total_score_sum_3_prior,
    n_grade_status_is_not_null_rank
FROM windowed
ORDER BY distinct_grade_status_sum_6_prior DESC
