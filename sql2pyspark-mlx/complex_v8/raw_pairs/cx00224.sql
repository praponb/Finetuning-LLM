WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 57.6 THEN 'large' WHEN score >= 42.4 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, st.cohort AS student_cohort
    FROM prepared_enrollments s
    JOIN students st ON st.id = s.student_id AND st.major IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(score) OVER (PARTITION BY score_tier ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_avg_1_prior,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY course_id, student_cohort ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        NTILE(5) OVER (PARTITION BY student_id, score_tier ORDER BY score DESC, id) AS score_quintile,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY student_cohort)) AS distinct_grade_status_in_student_cohort,
        MAX(score) OVER (PARTITION BY student_cohort) AS student_cohort_max_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_cohort), 0), 4) AS score_share_of_student_cohort,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, student_cohort ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist,
        ROUND(score - AVG(score) OVER w_student_cohort, 2) AS score_vs_student_cohort_avg,
        RANK() OVER w_score_tier_by_enrolled_at_ti AS enrolled_at_rank
    FROM with_parent
    WINDOW w_student_cohort AS (PARTITION BY student_cohort), w_score_tier_by_enrolled_at_ti AS (PARTITION BY score_tier ORDER BY enrolled_at DESC)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE enrolled_at_rank <= 3
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        score,
        grade_status,
        student_cohort,
        score_share_of_student_cohort,
        enrolled_at_cume_dist,
        enrolled_at_rank,
        DENSE_RANK() OVER (PARTITION BY enrolled_at_rank ORDER BY score_share_of_student_cohort, id) AS score_share_of_student_cohort_dense_rank,
        ROUND(score_quintile / NULLIF(SUM(score_quintile) OVER (PARTITION BY score_tier), 0), 4) AS score_quintile_share_of_score_tier,
        ROUND(enrolled_at_rank / NULLIF(SUM(enrolled_at_rank) OVER (PARTITION BY score_tier), 0), 4) AS enrolled_at_rank_share_of_score_tier,
        ROUND(score_quintile - AVG(score_quintile) OVER (PARTITION BY score_tier), 2) AS score_quintile_vs_score_tier_avg,
        NTILE(4) OVER w_all_by_score_avg_1_prior AS score_avg_1_prior_quartile,
        ROUND((UNIX_TIMESTAMP(last_enrolled_at) - UNIX_TIMESTAMP(LAG(last_enrolled_at) OVER (ORDER BY score_avg_1_prior DESC, id))) / 60, 1) AS minutes_since_prev_last_enrolled_at
    FROM top_per_group
    WINDOW w_all_by_score_avg_1_prior AS (ORDER BY score_avg_1_prior DESC, id)
),
stats AS (
    -- Summarise per group
    SELECT
        CASE WHEN enrolled_at_cume_dist >= 0.7 THEN 'HIGH' WHEN enrolled_at_cume_dist >= 0.2 THEN 'MEDIUM' ELSE 'LOW' END AS enrolled_at_cume_dist_tier,
        ROUND(SUM(score) / NULLIF(SUM(enrolled_at_rank_share_of_score_tier), 0), 3) AS score_to_enrolled_at_rank_share_of_score_tier_ra,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(student_cohort)) AS student_cohort_list,
        COUNT(DISTINCT student_cohort) AS distinct_student_cohorts,
        COUNT(*) FILTER (WHERE grade_status IS NOT NULL) AS n_grade_status_is_not_null
    FROM sequenced
    WHERE score_share_of_student_cohort_dense_rank > 2
    GROUP BY 1
    HAVING COUNT(*) >= 2
)
SELECT
    enrolled_at_cume_dist_tier,
    score_to_enrolled_at_rank_share_of_score_tier_ra,
    n_rows,
    distinct_student_cohorts,
    n_grade_status_is_not_null,
    COALESCE(distinct_student_cohorts, 0) AS distinct_student_cohorts_filled
FROM stats
ORDER BY enrolled_at_cume_dist_tier DESC NULLS FIRST, distinct_student_cohorts DESC
