WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        CASE WHEN score >= 57.6 THEN 'A' WHEN score >= 42.4 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE score <= 80.2
      AND enrolled_at >= TIMESTAMP '2025-01-31 14:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY grade_status, student_id ORDER BY score, id) AS seq_no,
        LAG(score, 1, 0) OVER (ORDER BY score_bucket, id) AS prev_score,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER w_course_id_term, 0), 4) AS score_bucket_share_of_course_id_term,
        ROUND(score_bucket - AVG(score_bucket) OVER (PARTITION BY course_id, term), 2) AS score_bucket_vs_course_id_term_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term ORDER BY score ASC NULLS LAST), 3) AS score_percent_rank,
        MIN(score) OVER (ORDER BY score_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        MAX(score) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_max_3,
        NTILE(10) OVER w_grade_status_by_score AS score_decile,
        SUM(score) OVER (ORDER BY score_bucket, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_sum_2_prior,
        DENSE_RANK() OVER (ORDER BY score_bucket, id) AS score_bucket_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY score ASC NULLS LAST, id) AS seq_no_2
    FROM filtered_enrollments
    WINDOW w_course_id_term AS (PARTITION BY course_id, term), w_grade_status_by_score AS (PARTITION BY grade_status ORDER BY score DESC, id)
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        score_bucket,
        score_tier,
        prev_score,
        score_bucket_share_of_course_id_term,
        score_bucket_vs_course_id_term_avg,
        score_percent_rank,
        min_score_so_far,
        score_max_3,
        score_decile,
        score_sum_2_prior,
        score_bucket_dense_rank,
        seq_no_2,
        ROUND(prev_score / NULLIF(SUM(prev_score) OVER w_score_bucket_dense_rank_grad, 0), 4) AS prev_score_share_of_score_bucket_dense_rank_grad,
        MAX(seq_no) OVER (PARTITION BY course_id, score_tier ORDER BY score_bucket_vs_course_id_term_avg, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS seq_no_max_1_prior,
        ROUND(CUME_DIST() OVER w_all_by_prev_score_ties, 3) AS prev_score_cume_dist,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER (PARTITION BY score_bucket_dense_rank, grade_status), 0), 4) AS score_bucket_share_of_score_bucket_dense_rank_gr
    FROM with_trends
    WINDOW w_score_bucket_dense_rank_grad AS (PARTITION BY score_bucket_dense_rank, grade_status), w_all_by_prev_score_ties AS (ORDER BY prev_score)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        score_bucket_dense_rank,
        score_tier,
        COUNT(*) AS row_count,
        SUM(prev_score) AS total_prev_score,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        ROUND(AVG(min_score_so_far), 2) AS avg_min_score_so_far
    FROM with_trends_2
    GROUP BY score_bucket_dense_rank, score_tier
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        score_bucket_dense_rank,
        score_tier,
        row_count,
        score_bucket_dense_rank - LAG(score_bucket_dense_rank) OVER (PARTITION BY score_bucket_dense_rank ORDER BY avg_min_score_so_far ASC, score_bucket_dense_rank, score_tier) AS score_bucket_dense_rank_change,
        MAX(score_bucket_dense_rank) OVER (PARTITION BY score_bucket_dense_rank, distinct_student_ids) AS score_bucket_dense_rank_distinct_student_ids_max,
        MIN(score_bucket_dense_rank) OVER (PARTITION BY score_bucket_dense_rank, distinct_student_ids) AS score_bucket_dense_rank_distinct_student_ids_min
    FROM rollup_base
)
SELECT
    score_bucket_dense_rank,
    score_tier,
    score_bucket_dense_rank_distinct_student_ids_max,
    score_bucket_dense_rank_distinct_student_ids_min
FROM sequenced
ORDER BY score_tier DESC, score_bucket_dense_rank_distinct_student_ids_max
