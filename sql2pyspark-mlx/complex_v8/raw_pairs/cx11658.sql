WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE course_id <= 6
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ROW_NUMBER() OVER (ORDER BY score ASC, id) AS score_seq,
        LEAD(score) OVER w_grade_status_course_id_by_sc AS next_score,
        NTILE(4) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at ASC, id) AS enrolled_at_quartile,
        DENSE_RANK() OVER (PARTITION BY grade_status, course_id ORDER BY score DESC) AS score_dense_rank,
        score - LAG(score) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at ASC, id) AS score_change,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY grade_status)) AS distinct_course_id_in_grade_status,
        ROW_NUMBER() OVER (ORDER BY enrolled_at DESC, id) AS rn,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id_enrolled_at_to_da, 0), 4) AS score_share_of_student_id_enrolled_at_to_date,
        COUNT(*) OVER (PARTITION BY student_id, TO_DATE(enrolled_at)) AS rows_in_student_id_enrolled_at_to_date,
        DENSE_RANK() OVER (PARTITION BY term, course_id ORDER BY enrolled_at) AS enrolled_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY term, course_id ORDER BY enrolled_at, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        SIZE(COLLECT_SET(course_id) OVER w_student_id_enrolled_at_to_da) AS distinct_course_id_in_student_id_enrolled_at_to_,
        MIN(score) OVER (PARTITION BY grade_status) AS grade_status_min_score,
        ROUND(PERCENT_RANK() OVER w_all_by_score, 3) AS score_percent_rank,
        LEAD(enrolled_at, 2) OVER (PARTITION BY term, course_id ORDER BY enrolled_at, id) AS next2_enrolled_at
    FROM filtered_enrollments
    WINDOW w_grade_status_course_id_by_sc AS (PARTITION BY grade_status, course_id ORDER BY score DESC, id), w_student_id_enrolled_at_to_da AS (PARTITION BY student_id, TO_DATE(enrolled_at)), w_all_by_score AS (ORDER BY score ASC, id)
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        score,
        next_score,
        score_dense_rank,
        distinct_course_id_in_grade_status,
        rn,
        score_share_of_student_id_enrolled_at_to_date,
        enrolled_at_dense_rank,
        seq_no,
        distinct_course_id_in_student_id_enrolled_at_to_,
        score_percent_rank,
        next2_enrolled_at,
        RANK() OVER (ORDER BY enrolled_at_dense_rank DESC, id) AS enrolled_at_dense_rank_rank,
        MIN(score_percent_rank) OVER (PARTITION BY rn, enrolled_at_dense_rank) AS rn_enrolled_at_dense_rank_min_score_percent_rank,
        ROUND(AVG(rows_in_grade_status) OVER (ORDER BY enrolled_at_dense_rank DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS rows_in_grade_status_avg_1_prior,
        LAST_VALUE(enrolled_at_quartile) OVER (ORDER BY enrolled_at_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at_quartile,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at_dense_rank DESC), 3) AS enrolled_at_dense_rank_cume_dist,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY rn, enrolled_at_dense_rank)) AS distinct_course_id_in_rn_enrolled_at_dense_rank,
        SUM(score_seq) OVER (ORDER BY next_score DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_seq_sum_2,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY rn, enrolled_at_dense_rank)) AS distinct_term_in_rn_enrolled_at_dense_rank
    FROM metrics
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score_dense_rank,
    distinct_course_id_in_grade_status,
    rn,
    score_share_of_student_id_enrolled_at_to_date,
    enrolled_at_dense_rank,
    distinct_course_id_in_student_id_enrolled_at_to_,
    score_percent_rank,
    next2_enrolled_at,
    enrolled_at_dense_rank_rank,
    rows_in_grade_status_avg_1_prior,
    last_enrolled_at_quartile,
    enrolled_at_dense_rank_cume_dist,
    distinct_course_id_in_rn_enrolled_at_dense_rank,
    score_seq_sum_2,
    distinct_term_in_rn_enrolled_at_dense_rank
FROM metrics_2
ORDER BY score_percent_rank, enrolled_at_dense_rank_cume_dist DESC NULLS FIRST
