WITH prepared_enrollments AS (
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
    WHERE score >= 20.2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUM(score) OVER (PARTITION BY course_id ORDER BY score NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_sum_4,
        ROUND(AVG(score) OVER (PARTITION BY course_id ORDER BY score, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        ROUND(score / NULLIF(SUM(score) OVER w_grade_status_term, 0), 4) AS score_share_of_grade_status_term,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        MAX(score) OVER (PARTITION BY course_id ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        ROUND(AVG(score) OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_avg_3,
        ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY score NULLS LAST, id) AS rn,
        MIN(score) OVER (PARTITION BY grade_status, term) AS grade_status_term_min_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_2,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY score NULLS LAST, id) AS score_dense_rank,
        MAX(score) OVER (PARTITION BY student_id, term) AS student_id_term_max_score,
        SUM(score) OVER (PARTITION BY student_id, term) AS student_id_term_sum_score,
        ROUND(score - AVG(score) OVER w_student_id_term, 2) AS score_vs_student_id_term_avg,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, term), 2) AS grade_status_term_avg_score,
        COUNT(*) OVER (PARTITION BY grade_status, term) AS rows_in_grade_status_term
    FROM prepared_enrollments
    WINDOW w_grade_status_term AS (PARTITION BY grade_status, term), w_student_id_term AS (PARTITION BY student_id, term)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(second_score_2) OVER (ORDER BY score_dense_rank DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS second_score_2_max_2_prior,
        SUM(score_avg_7) OVER (PARTITION BY course_id, score_dense_rank ORDER BY score_vs_student_id_term_avg, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_avg_7_sum_1_prior,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY MONTH(enrolled_at))) AS distinct_course_id_in_enrolled_at_month,
        LAG(score_dense_rank, 3) OVER (PARTITION BY course_id, score_dense_rank ORDER BY score_vs_student_id_term_avg, id) AS prev3_score_dense_rank,
        NTH_VALUE(second_score, 2) OVER (PARTITION BY course_id, score_dense_rank ORDER BY score_vs_student_id_term_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_score,
        score_sum_4 - LAG(score_sum_4) OVER w_all_by_score_dense_rank AS score_sum_4_change
    FROM ranked
    WINDOW w_all_by_score_dense_rank AS (ORDER BY score_dense_rank DESC, id)
)
SELECT
    id,
    course_id,
    term,
    score_sum_4,
    score_share_of_grade_status_term,
    second_enrolled_at,
    max_score_so_far,
    score_avg_3,
    rn,
    second_score_2,
    score_dense_rank,
    student_id_term_max_score,
    student_id_term_sum_score,
    score_vs_student_id_term_avg,
    grade_status_term_avg_score,
    rows_in_grade_status_term,
    score_avg_7_sum_1_prior,
    distinct_course_id_in_enrolled_at_month,
    prev3_score_dense_rank,
    second_second_score,
    score_sum_4_change,
    DATE_TRUNC('QUARTER', second_enrolled_at) AS second_enrolled_at_quarter,
    LENGTH(TRIM(grade_status)) AS grade_status_len
FROM with_windows
WHERE score_share_of_grade_status_term <= 0.7
ORDER BY second_second_score, rows_in_grade_status_term DESC
