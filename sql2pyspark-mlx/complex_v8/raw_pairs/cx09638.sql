WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN', 'PASS')
      AND score BETWEEN 8.7 AND 66.0
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY grade_status, student_id ORDER BY score, id) AS score_dense_rank,
        COUNT(*) OVER (PARTITION BY course_id, term) AS rows_in_course_id_term,
        MAX(score) OVER (PARTITION BY student_id) AS student_id_max_score,
        ROW_NUMBER() OVER (PARTITION BY student_id, grade_status ORDER BY score DESC, id) AS rn,
        LAST_VALUE(score_filled) OVER (PARTITION BY course_id ORDER BY score ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_filled,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY student_id)) AS distinct_course_id_in_student_id,
        ROUND(score_filled - AVG(score_filled) OVER (PARTITION BY course_id, term), 2) AS score_filled_vs_course_id_term_avg,
        ROW_NUMBER() OVER w_term_grade_status_by_score_f AS rn_2,
        SUM(score_filled) OVER (PARTITION BY term, grade_status ORDER BY score_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_filled
    FROM cleaned_enrollments
    WINDOW w_course_id_term AS (PARTITION BY course_id, term), w_term_grade_status_by_score_f AS (PARTITION BY term, grade_status ORDER BY score_filled DESC, id)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        score_filled,
        rows_in_course_id_term,
        student_id_max_score,
        last_score_filled,
        score_filled_vs_course_id_term_avg,
        rn_2,
        running_score_filled,
        ROW_NUMBER() OVER (PARTITION BY distinct_course_id_in_student_id ORDER BY score_dense_rank DESC, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY distinct_course_id_in_student_id ORDER BY score_dense_rank DESC, id) AS score_dense_rank_dense_rank,
        SUM(last_score_filled) OVER w_student_id AS student_id_sum_last_score_filled,
        DENSE_RANK() OVER w_grade_status_rn_by_distinct AS distinct_course_id_in_student_id_dense_rank,
        NTH_VALUE(student_id_max_score, 2) OVER (PARTITION BY grade_status, rn ORDER BY distinct_course_id_in_student_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_id_max_score,
        SUM(score) OVER (PARTITION BY student_id) AS student_id_sum_score,
        MAX(score_filled) OVER (PARTITION BY student_id) AS student_id_max_score_filled,
        COUNT(*) OVER w_student_id AS rows_in_student_id
    FROM sequenced
    WINDOW w_student_id AS (PARTITION BY student_id), w_grade_status_rn_by_distinct AS (PARTITION BY grade_status, rn ORDER BY distinct_course_id_in_student_id NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    enrolled_at,
    score_filled,
    rows_in_course_id_term,
    score_filled_vs_course_id_term_avg,
    rn_2,
    running_score_filled,
    seq_no,
    score_dense_rank_dense_rank,
    distinct_course_id_in_student_id_dense_rank,
    second_student_id_max_score,
    student_id_sum_score,
    student_id_max_score_filled,
    rows_in_student_id
FROM sequenced_2
WHERE score_filled_vs_course_id_term_avg BETWEEN -60.0 AND 90.0
ORDER BY student_id_sum_score
