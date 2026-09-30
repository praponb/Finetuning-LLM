WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref
    FROM enrollments
    WHERE student_id <= 17
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term, student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        SIZE(COLLECT_SET(term) OVER w_course_id_enrolled_at_year) AS distinct_term_in_course_id_enrolled_at_year,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status ORDER BY score DESC, id), 3) AS score_cume_dist,
        MIN(score) OVER (PARTITION BY course_id, YEAR(enrolled_at)) AS course_id_enrolled_at_year_min_score
    FROM recent
    WINDOW w_course_id_enrolled_at_year AS (PARTITION BY course_id, YEAR(enrolled_at))
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(score_cume_dist) OVER (PARTITION BY YEAR(second_enrolled_at)) AS second_enrolled_at_year_min_score_cume_dist,
        LAST_VALUE(score) IGNORE NULLS OVER (ORDER BY second_enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        NTH_VALUE(second_enrolled_at, 2) OVER (ORDER BY second_enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_enrolled_at,
        NTILE(4) OVER (ORDER BY second_enrolled_at DESC, id) AS second_enrolled_at_quartile,
        MAX(score) OVER (ORDER BY second_enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        COUNT(*) OVER (PARTITION BY YEAR(second_enrolled_at)) AS rows_in_second_enrolled_at_year
    FROM metrics
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        term,
        score,
        enrolled_at,
        term_ref,
        second_enrolled_at,
        score_cume_dist,
        course_id_enrolled_at_year_min_score,
        last_known_score,
        second_second_enrolled_at,
        max_score_so_far,
        SUM(max_score_so_far) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at)) AS enrolled_at_month_sum_max_score_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term ORDER BY distinct_term_in_course_id_enrolled_at_year), 3) AS distinct_term_in_course_id_enrolled_at_year_perc,
        ROUND(score_cume_dist / NULLIF(SUM(score_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at)), 0), 4) AS score_cume_dist_share_of_enrolled_at_month,
        NTH_VALUE(last_known_score, 2) OVER (PARTITION BY term ORDER BY distinct_term_in_course_id_enrolled_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_score
    FROM with_trends
)
-- Final projection
SELECT
    id,
    term,
    score,
    enrolled_at,
    second_enrolled_at,
    score_cume_dist,
    last_known_score,
    second_second_enrolled_at,
    second_last_known_score,
    SUBSTRING(TRIM(term_ref), 1, 2) AS term_ref_prefix
FROM with_trends_2
