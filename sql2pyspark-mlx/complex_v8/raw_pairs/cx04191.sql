WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE course_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        NTILE(5) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_quintile,
        LEAD(score) OVER w_student_id_by_enrolled_at AS next_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, course_id), 0), 4) AS score_share_of_grade_status_course_id,
        NTILE(3) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_tercile,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at DESC NULLS FIRST), 3) AS enrolled_at_percent_rank,
        ROUND(AVG(score) OVER w_grade_status_course_id, 2) AS grade_status_course_id_avg_score,
        LAG(score, 2, 0) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS prev2_score,
        SUM(score) OVER (PARTITION BY grade_status, course_id) AS grade_status_course_id_sum_score
    FROM base
    WINDOW w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at, id), w_grade_status_course_id AS (PARTITION BY grade_status, course_id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        enrolled_at_tercile,
        CASE WHEN first_known_score >= 79.2 THEN 'A' WHEN first_known_score >= 38.1 THEN 'B' ELSE 'C' END AS first_known_score_tier,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        SUM(enrolled_at_quintile) AS total_enrolled_at_quintile,
        COUNT(*) FILTER (WHERE term IS NOT NULL) AS n_term_is_not_null
    FROM ranked
    WHERE score_share_of_grade_status_course_id > 0.1
    GROUP BY enrolled_at_tercile, CASE WHEN first_known_score >= 79.2 THEN 'A' WHEN first_known_score >= 38.1 THEN 'B' ELSE 'C' END
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY enrolled_at_tercile ORDER BY n_term_is_not_null NULLS LAST, enrolled_at_tercile, first_known_score_tier) AS n_term_is_not_null_seq,
        RANK() OVER (ORDER BY distinct_grade_status, enrolled_at_tercile, first_known_score_tier) AS distinct_grade_status_rank
    FROM per_group
)
-- Final projection
SELECT
    enrolled_at_tercile,
    first_known_score_tier,
    total_enrolled_at_quintile,
    n_term_is_not_null,
    n_term_is_not_null_seq,
    distinct_grade_status_rank,
    CASE WHEN distinct_grade_status >= 8 THEN 'HIGH' WHEN distinct_grade_status >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_grade_status_tier,
    COALESCE(first_known_score_tier, 'UNKNOWN') AS first_known_score_tier_or_unknown
FROM ranked_2
ORDER BY enrolled_at_tercile DESC, first_known_score_tier
