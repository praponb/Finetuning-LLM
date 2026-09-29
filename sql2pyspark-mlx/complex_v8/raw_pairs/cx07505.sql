WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE course_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY grade_status, term ORDER BY score ASC, id) AS seq_no,
        SUM(score) OVER (PARTITION BY TO_DATE(enrolled_at), course_id ORDER BY days_since_enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_sum_2_prior,
        ROUND(days_since_enrolled_at - AVG(days_since_enrolled_at) OVER (PARTITION BY course_id), 2) AS days_since_enrolled_at_vs_course_id_avg,
        SIZE(COLLECT_SET(grade_status) OVER w_student_id_course_id) AS distinct_grade_status_in_student_id_course_id,
        SUM(days_since_enrolled_at) OVER (ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_enrolled_at_sum_1_prior,
        RANK() OVER w_all_by_score_ties AS score_rank,
        ROUND(CUME_DIST() OVER (ORDER BY score, id), 3) AS score_cume_dist,
        SUM(days_since_enrolled_at) OVER (ORDER BY score, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_enrolled_at_sum_7,
        MAX(days_since_enrolled_at) OVER w_student_id_course_id AS student_id_course_id_max_days_since_enrolled_at,
        LEAD(score_bucket) OVER (PARTITION BY term, grade_status ORDER BY score_bucket DESC, id) AS next_score_bucket,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id, course_id), 0), 4) AS score_share_of_student_id_course_id,
        DENSE_RANK() OVER w_grade_status_term_by_score_t AS score_dense_rank,
        NTILE(10) OVER (PARTITION BY term, grade_status ORDER BY score_bucket DESC, id) AS score_bucket_decile,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY term, grade_status ORDER BY score_bucket DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(AVG(score_bucket) OVER (PARTITION BY student_id, course_id), 2) AS student_id_course_id_avg_score_bucket
    FROM filtered_enrollments
    WINDOW w_student_id_course_id AS (PARTITION BY student_id, course_id), w_all_by_score_ties AS (ORDER BY score), w_grade_status_term_by_score_t AS (PARTITION BY grade_status, term ORDER BY score ASC)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(grade_status, '#', CAST(score_bucket AS STRING)) AS grade_status_ref
    FROM windowed
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        term,
        distinct_grade_status_in_student_id_course_id,
        SUM(days_since_enrolled_at_sum_7) AS total_days_since_enrolled_at_sum_7,
        COUNT(*) FILTER (WHERE term IS NOT NULL) AS n_term_is_not_null,
        ROUND(STDDEV(score_sum_2_prior), 2) AS stddev_score_sum_2_prior,
        ROUND(AVG(minutes_since_prev_enrolled_at), 2) AS avg_minutes_since_prev_enrolled_at
    FROM enriched
    GROUP BY term, distinct_grade_status_in_student_id_course_id
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(n_term_is_not_null - AVG(n_term_is_not_null) OVER (PARTITION BY distinct_grade_status_in_student_id_course_id, term), 2) AS n_term_is_not_null_vs_distinct_grade_status_in_s,
        ROW_NUMBER() OVER (ORDER BY stddev_score_sum_2_prior DESC NULLS FIRST, term, distinct_grade_status_in_student_id_course_id) AS seq_no,
        LEAD(stddev_score_sum_2_prior, 3, 0) OVER w_all_by_stddev_score_sum_2_pr AS next3_stddev_score_sum_2_prior,
        NTH_VALUE(n_term_is_not_null, 2) OVER (ORDER BY stddev_score_sum_2_prior DESC NULLS FIRST, term, distinct_grade_status_in_student_id_course_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_term_is_not_null
    FROM summary
    WINDOW w_all_by_stddev_score_sum_2_pr AS (ORDER BY stddev_score_sum_2_prior DESC NULLS FIRST, term, distinct_grade_status_in_student_id_course_id)
)
SELECT
    term,
    distinct_grade_status_in_student_id_course_id,
    total_days_since_enrolled_at_sum_7,
    stddev_score_sum_2_prior,
    avg_minutes_since_prev_enrolled_at,
    next3_stddev_score_sum_2_prior,
    second_n_term_is_not_null,
    CASE WHEN term IN ('2024T2', '2025T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
FROM windowed_2
ORDER BY avg_minutes_since_prev_enrolled_at DESC NULLS FIRST
