WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score BETWEEN 8.7 AND 59.7
      AND term IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_score AS seq_no,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY term, student_id), 0), 4) AS score_share_of_term_student_id,
        COUNT(*) OVER (PARTITION BY term, student_id) AS rows_in_term_student_id,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        MIN(score) OVER (PARTITION BY grade_status, term) AS grade_status_term_min_score,
        SUM(score) OVER w_grade_status AS grade_status_sum_score,
        ROUND(AVG(score) OVER (PARTITION BY grade_status), 2) AS grade_status_avg_score,
        DENSE_RANK() OVER w_all_by_score AS score_dense_rank,
        ROUND(AVG(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        MAX(score) OVER (PARTITION BY grade_status, term) AS grade_status_term_max_score,
        SIZE(COLLECT_SET(student_id) OVER w_grade_status) AS distinct_student_id_in_grade_status,
        LAST_VALUE(score) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        DENSE_RANK() OVER w_course_id_by_enrolled_at_tie AS enrolled_at_dense_rank
    FROM recent
    WINDOW w_all_by_score AS (ORDER BY score ASC, id), w_grade_status AS (PARTITION BY grade_status), w_course_id_by_enrolled_at_tie AS (PARTITION BY course_id ORDER BY enrolled_at)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        term,
        score,
        grade_status,
        score_share_of_term_student_id,
        rows_in_term_student_id,
        rows_in_grade_status,
        grade_status_term_min_score,
        grade_status_sum_score,
        grade_status_avg_score,
        score_dense_rank,
        score_avg_6_prior,
        grade_status_term_max_score,
        distinct_student_id_in_grade_status,
        enrolled_at_dense_rank,
        LEAD(seq_no, 2) OVER (PARTITION BY distinct_student_id_in_grade_status, course_id ORDER BY enrolled_at_dense_rank, id) AS next2_seq_no,
        SUM(score_avg_6_prior) OVER (PARTITION BY distinct_student_id_in_grade_status, course_id ORDER BY enrolled_at_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY student_id ORDER BY last_score DESC) AS last_score_dense_rank
    FROM metrics
)
-- Final projection
SELECT
    id,
    score,
    score_share_of_term_student_id,
    rows_in_grade_status,
    grade_status_term_min_score,
    grade_status_sum_score,
    score_dense_rank,
    score_avg_6_prior,
    grade_status_term_max_score,
    distinct_student_id_in_grade_status,
    next2_seq_no,
    CONCAT(term, '#', CAST(next2_seq_no AS STRING)) AS term_ref
FROM with_windows
ORDER BY rows_in_grade_status, score_dense_rank DESC, id
LIMIT 20
