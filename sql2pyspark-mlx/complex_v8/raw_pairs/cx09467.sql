WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-03-02 09:00:00'
      AND student_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(score) OVER (PARTITION BY grade_status) AS grade_status_min_score,
        NTILE(4) OVER (PARTITION BY grade_status ORDER BY score_filled, id) AS score_filled_quartile,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status), 0), 4) AS score_share_of_grade_status,
        MAX(score_filled) OVER (PARTITION BY grade_status) AS grade_status_max_score_filled,
        RANK() OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_rank,
        ROUND(AVG(score_filled) OVER (PARTITION BY grade_status ORDER BY score_filled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_filled_avg_2_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at DESC), 3) AS enrolled_at_percent_rank,
        ROUND(AVG(score_filled) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_filled_avg_3,
        SIZE(COLLECT_SET(student_id) OVER w_grade_status) AS distinct_student_id_in_grade_status,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status ORDER BY score_filled), 3) AS score_filled_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_percent_rank_2,
        score - LAG(score) OVER w_all_by_enrolled_at AS score_change,
        ROUND(CUME_DIST() OVER w_grade_status_by_enrolled_at, 3) AS enrolled_at_cume_dist
    FROM filtered
    WINDOW w_grade_status AS (PARTITION BY grade_status), w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id), w_grade_status_by_enrolled_at AS (PARTITION BY grade_status ORDER BY enrolled_at DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE enrolled_at_rank = 1
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        grade_status,
        score_filled_quartile,
        enrolled_at_rank,
        MIN(score_filled_quartile) AS min_score_filled_quartile,
        ROUND(AVG(enrolled_at_percent_rank), 2) AS avg_enrolled_at_percent_rank,
        MAX(enrolled_at) AS last_enrolled_at,
        ROUND(SUM(score_filled_cume_dist), 2) AS total_score_filled_cume_dist,
        ROUND(SUM(score_change) / NULLIF(SUM(enrolled_at_percent_rank), 0), 3) AS score_change_to_enrolled_at_percent_rank_ratio
    FROM top_per_group
    GROUP BY grade_status, score_filled_quartile, enrolled_at_rank
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(avg_enrolled_at_percent_rank) OVER (PARTITION BY score_filled_quartile) AS score_filled_quartile_sum_avg_enrolled_at_percen,
        ROUND(CUME_DIST() OVER (PARTITION BY enrolled_at_rank ORDER BY avg_enrolled_at_percent_rank, grade_status, score_filled_quartile, enrolled_at_rank), 3) AS avg_enrolled_at_percent_rank_cume_dist
    FROM rollup_base
)
SELECT
    grade_status,
    enrolled_at_rank,
    min_score_filled_quartile,
    avg_enrolled_at_percent_rank,
    last_enrolled_at,
    YEAR(last_enrolled_at) AS last_enrolled_at_year
FROM ranked
WHERE grade_status IS NOT NULL
ORDER BY avg_enrolled_at_percent_rank, last_enrolled_at DESC
