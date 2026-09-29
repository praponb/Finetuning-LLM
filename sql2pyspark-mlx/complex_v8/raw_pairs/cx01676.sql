WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 08:00:00'
),
enriched_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        c.course_code,
        st.enrolled_on AS student_enrolled_on
    FROM recent s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_score AS seq_no,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY student_enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        MIN(score) OVER (PARTITION BY course_id) AS course_id_min_score,
        LAG(score) OVER (PARTITION BY student_id ORDER BY student_enrolled_on, id) AS prev_score,
        ROUND(AVG(score) OVER w_course_id, 2) AS course_id_avg_score,
        LAG(student_enrolled_on, 2) OVER (ORDER BY score DESC, id) AS prev2_student_enrolled_on,
        LAST_VALUE(student_enrolled_on) OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_student_enrolled_on,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        LEAD(student_enrolled_on) OVER (ORDER BY score DESC, id) AS next_student_enrolled_on,
        DENSE_RANK() OVER (PARTITION BY student_id ORDER BY student_enrolled_on) AS student_enrolled_on_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id ORDER BY score, id), 3) AS score_percent_rank,
        SUM(score) OVER (PARTITION BY student_id ORDER BY student_enrolled_on, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_sum_3_prior,
        COUNT(*) OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM enriched_course
    WINDOW w_all_by_score AS (ORDER BY score DESC, id), w_course_id AS (PARTITION BY course_id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        course_code,
        student_enrolled_on,
        seq_no,
        last_known_score,
        course_id_min_score,
        prev_score,
        course_id_avg_score,
        prev2_student_enrolled_on,
        last_student_enrolled_on,
        rows_in_course_id,
        next_student_enrolled_on,
        score_percent_rank,
        score_sum_3_prior,
        running_count
    FROM ranked
    WHERE student_enrolled_on_dense_rank = 1
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        course_code,
        student_enrolled_on,
        seq_no,
        last_known_score,
        course_id_avg_score,
        prev2_student_enrolled_on,
        last_student_enrolled_on,
        rows_in_course_id,
        next_student_enrolled_on,
        score_percent_rank,
        score_sum_3_prior,
        running_count,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY YEAR(prev2_student_enrolled_on), student_id)) AS distinct_grade_status_in_prev2_student_enrolled_,
        COUNT(*) OVER w_prev2_student_enrolled_on_ye AS rows_in_prev2_student_enrolled_on_year_student_i,
        MAX(score) OVER (PARTITION BY MONTH(next_student_enrolled_on) ORDER BY rows_in_course_id, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_max_2_prior
    FROM top_rows
    WINDOW w_prev2_student_enrolled_on_ye AS (PARTITION BY YEAR(prev2_student_enrolled_on), student_id)
)
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    course_code,
    student_enrolled_on,
    seq_no,
    last_known_score,
    course_id_avg_score,
    prev2_student_enrolled_on,
    last_student_enrolled_on,
    rows_in_course_id,
    next_student_enrolled_on,
    score_sum_3_prior,
    distinct_grade_status_in_prev2_student_enrolled_,
    rows_in_prev2_student_enrolled_on_year_student_i,
    score_max_2_prior
FROM ranked_2
ORDER BY grade_status DESC
