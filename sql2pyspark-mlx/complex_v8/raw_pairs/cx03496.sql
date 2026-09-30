WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE score BETWEEN 32.9 AND 92.4
      AND grade_status IN ('FAIL', 'PASS')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status_ref,
        grade_status_len,
        LAG(score) OVER w_all_by_score AS prev_score,
        ROW_NUMBER() OVER (PARTITION BY grade_status, grade_status_len ORDER BY grade_status_len, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY term, course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(score) OVER (PARTITION BY grade_status, grade_status_len ORDER BY grade_status_len, id) AS prev_score_2,
        DENSE_RANK() OVER (PARTITION BY student_id ORDER BY score DESC NULLS FIRST) AS score_dense_rank,
        ROUND(grade_status_len - AVG(grade_status_len) OVER w_grade_status_len_course_id, 2) AS grade_status_len_vs_grade_status_len_course_id_a,
        COUNT(*) OVER w_grade_status_len_course_id AS rows_in_grade_status_len_course_id,
        LAG(score, 2) OVER (PARTITION BY grade_status, grade_status_len ORDER BY grade_status_len, id) AS prev2_score
    FROM recent
    WINDOW w_all_by_score AS (ORDER BY score DESC, id), w_grade_status_len_course_id AS (PARTITION BY grade_status_len, course_id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        grade_status_ref,
        grade_status_len,
        prev_score,
        seq_no,
        running_count,
        grade_status_len_vs_grade_status_len_course_id_a,
        rows_in_grade_status_len_course_id,
        SUM(score_dense_rank) OVER (PARTITION BY seq_no) AS seq_no_sum_score_dense_rank,
        COUNT(*) OVER (PARTITION BY score_dense_rank) AS rows_in_score_dense_rank,
        COUNT(*) OVER (PARTITION BY seq_no) AS rows_in_seq_no,
        MAX(running_count) OVER (PARTITION BY seq_no) AS seq_no_max_running_count,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY seq_no)) AS distinct_term_in_seq_no,
        MIN(prev_score_2) OVER (PARTITION BY score_dense_rank) AS score_dense_rank_min_prev_score_2
    FROM with_windows
),
summary AS (
    -- Summarise per group
    SELECT
        seq_no,
        student_id,
        distinct_term_in_seq_no,
        ROUND(SUM(grade_status_len_vs_grade_status_len_course_id_a) / NULLIF(SUM(running_count), 0), 3) AS grade_status_len_vs_grade_status_len_course_id_a_2,
        ROUND(AVG(score_dense_rank_min_prev_score_2), 2) AS avg_score_dense_rank_min_prev_score_2,
        ROUND(STDDEV(grade_status_len), 2) AS stddev_grade_status_len,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(prev_score) AS max_prev_score,
        PERCENTILE_APPROX(rows_in_seq_no, 0.5) AS median_rows_in_seq_no
    FROM metrics
    GROUP BY 1, 2, 3
)
SELECT
    seq_no,
    student_id,
    distinct_term_in_seq_no,
    grade_status_len_vs_grade_status_len_course_id_a_2,
    stddev_grade_status_len,
    distinct_ids,
    max_prev_score
FROM summary
ORDER BY distinct_ids DESC, seq_no DESC
