WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix
    FROM enrollments
    WHERE score >= 6.2
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        c.course_code,
        c.credits AS course_credits,
        c.level AS course_level
    FROM recent s
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(course_credits) OVER (PARTITION BY course_level ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_course_credits,
        ROUND(CUME_DIST() OVER w_course_id_course_credits_by, 3) AS course_credits_cume_dist,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_id, course_credits ORDER BY course_credits, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS course_credits_avg_2_prior,
        LAG(course_credits, 2) OVER (PARTITION BY course_level ORDER BY score, id) AS prev2_course_credits,
        ROUND(AVG(course_credits) OVER (PARTITION BY term, student_id ORDER BY score ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS course_credits_avg_1_prior,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_credits, course_id), 2) AS course_credits_course_id_avg_course_credits,
        DENSE_RANK() OVER (PARTITION BY term, student_id ORDER BY score ASC) AS score_dense_rank,
        COUNT(*) OVER (PARTITION BY course_credits, course_id) AS rows_in_course_credits_course_id,
        ROW_NUMBER() OVER (PARTITION BY course_level ORDER BY score, id) AS rn
    FROM with_details_course
    WINDOW w_course_id_course_credits_by AS (PARTITION BY course_id, course_credits ORDER BY course_credits)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term,
        score,
        course_level,
        first_course_credits,
        course_credits_cume_dist,
        score_dense_rank,
        rows_in_course_credits_course_id,
        rn,
        ROW_NUMBER() OVER w_all_by_course_credits_cume_d AS rn_2,
        LEAD(course_credits) OVER w_all_by_course_credits_cume_d AS next_course_credits,
        ROUND(CUME_DIST() OVER (PARTITION BY prev2_course_credits, course_credits ORDER BY score_dense_rank), 3) AS score_dense_rank_cume_dist,
        MAX(first_course_credits) OVER w_course_id_rn AS course_id_rn_max_first_course_credits,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY course_credits_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        ROUND(AVG(prev2_course_credits) OVER (PARTITION BY course_id, rn), 2) AS course_id_rn_avg_prev2_course_credits
    FROM metrics
    WINDOW w_all_by_course_credits_cume_d AS (ORDER BY course_credits_cume_dist, id), w_course_id_rn AS (PARTITION BY course_id, rn)
),
grouped AS (
    -- Group-level totals
    SELECT
        rn_2,
        SUM(CASE WHEN course_level <> 'core' THEN score ELSE 0 END) AS score_course_level_core,
        MIN(first_known_score) AS min_first_known_score,
        ARRAY_SORT(COLLECT_SET(course_level)) AS course_level_list,
        COUNT(DISTINCT term) AS distinct_terms
    FROM with_windows
    GROUP BY rn_2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY distinct_terms, rn_2 ORDER BY score_course_level_core, rn_2) AS score_course_level_core_rank,
        ROUND(AVG(min_first_known_score) OVER (PARTITION BY distinct_terms, rn_2), 2) AS distinct_terms_rn_2_avg_min_first_known_score,
        LAG(rn_2, 1, 0) OVER (PARTITION BY distinct_terms, rn_2 ORDER BY score_course_level_core, rn_2) AS prev_rn_2,
        RANK() OVER w_distinct_terms_rn_2_by_score_2 AS score_course_level_core_rank_2
    FROM grouped
    WINDOW w_distinct_terms_rn_2_by_score_2 AS (PARTITION BY distinct_terms, rn_2 ORDER BY score_course_level_core)
)
SELECT
    rn_2,
    distinct_terms,
    distinct_terms_rn_2_avg_min_first_known_score,
    score_course_level_core_rank_2,
    ROUND(score_course_level_core / NULLIF(prev_rn_2, 0), 3) AS score_course_level_core_per_prev_rn_2
FROM sequenced
ORDER BY score_course_level_core_per_prev_rn_2, distinct_terms
