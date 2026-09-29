WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 15 MINUTES AS enrolled_at_plus_15m,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
    WHERE grade_status IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        enrolled_at_plus_15m,
        term_ref,
        term_prefix,
        RANK() OVER (PARTITION BY term ORDER BY score, id) AS score_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY score, id), 3) AS score_cume_dist,
        NTILE(4) OVER (PARTITION BY term ORDER BY score DESC, id) AS score_quartile,
        RANK() OVER (PARTITION BY term ORDER BY score DESC) AS score_rank_2
    FROM scoped_enrollments
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        term_ref,
        score_rank,
        score_cume_dist,
        score_rank_2,
        ROUND(score_cume_dist / NULLIF(SUM(score_cume_dist) OVER (PARTITION BY score_rank_2, score_quartile), 0), 4) AS score_cume_dist_share_of_score_rank_2_score_quar,
        NTILE(10) OVER (PARTITION BY TO_DATE(enrolled_at_plus_15m) ORDER BY enrolled_at_plus_15m, id) AS enrolled_at_plus_15m_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY score_quartile ORDER BY enrolled_at_plus_15m), 3) AS enrolled_at_plus_15m_percent_rank,
        ROUND(score_rank_2 - AVG(score_rank_2) OVER (PARTITION BY score_rank_2, score_quartile), 2) AS score_rank_2_vs_score_rank_2_score_quartile_avg,
        ROUND(score_cume_dist - AVG(score_cume_dist) OVER (PARTITION BY score_rank_2, score_quartile), 2) AS score_cume_dist_vs_score_rank_2_score_quartile_a
    FROM ranked
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        term_ref,
        score_rank,
        score_cume_dist,
        score_cume_dist_share_of_score_rank_2_score_quar,
        enrolled_at_plus_15m_decile,
        enrolled_at_plus_15m_percent_rank,
        score_rank_2_vs_score_rank_2_score_quartile_avg,
        score_cume_dist_vs_score_rank_2_score_quartile_a
    FROM sequenced
    WHERE score_rank_2 = 1
      AND term_ref IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    enrolled_at_plus_15m_decile,
    score_rank,
    PERCENTILE_APPROX(score_rank, 0.5) AS median_score_rank,
    COUNT(*) AS cnt
FROM top_per_group
GROUP BY GROUPING SETS ((enrolled_at_plus_15m_decile, score_rank), (enrolled_at_plus_15m_decile), (score_rank), ())
ORDER BY enrolled_at_plus_15m_decile NULLS LAST, score_rank NULLS LAST
