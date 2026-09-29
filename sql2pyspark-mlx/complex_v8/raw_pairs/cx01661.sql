WITH cleaned_enrollments AS (
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
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
      AND score <= 87.8
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        NTILE(3) OVER w_student_id_by_enrolled_at AS enrolled_at_tercile,
        SIZE(COLLECT_SET(course_id) OVER w_term_grade_status) AS distinct_course_id_in_term_grade_status,
        MIN(score) OVER w_student_id_course_id AS student_id_course_id_min_score,
        FIRST_VALUE(score) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        ROW_NUMBER() OVER (ORDER BY score DESC, id) AS rn,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY term, grade_status), 0), 4) AS score_share_of_term_grade_status,
        NTH_VALUE(score, 2) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(AVG(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        SUM(score) OVER w_student_id_course_id AS student_id_course_id_sum_score,
        NTILE(4) OVER (ORDER BY score, id) AS score_quartile,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY enrolled_at, id) AS enrolled_at_dense_rank
    FROM cleaned_enrollments
    WINDOW w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at DESC, id), w_term_grade_status AS (PARTITION BY term, grade_status), w_student_id_course_id AS (PARTITION BY student_id, course_id)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_tercile,
        distinct_course_id_in_term_grade_status,
        student_id_course_id_min_score,
        first_score,
        score_share_of_term_grade_status,
        second_score,
        score_avg_3_prior,
        student_id_course_id_sum_score,
        score_quartile,
        enrolled_at_dense_rank
    FROM sequenced
    WHERE rn <= 5
)
SELECT
    id,
    term,
    score,
    enrolled_at,
    enrolled_at_tercile,
    distinct_course_id_in_term_grade_status,
    student_id_course_id_min_score,
    first_score,
    score_share_of_term_grade_status,
    second_score,
    score_quartile,
    enrolled_at_dense_rank,
    FLOOR(enrolled_at_dense_rank / 2) * 2 AS enrolled_at_dense_rank_bucket,
    GREATEST(first_score, score) AS greatest_first_score_score
FROM leaders
WHERE first_score <= 79.2
ORDER BY first_score, score_share_of_term_grade_status DESC
