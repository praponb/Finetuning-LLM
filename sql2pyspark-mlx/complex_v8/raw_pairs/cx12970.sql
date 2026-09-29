WITH base AS (
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
    WHERE student_id IS NOT NULL
      AND enrolled_at >= TIMESTAMP '2025-02-06 11:00:00'
),
joined_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        st.major AS student_major,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa,
        c.course_code
    FROM base s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY score, id) AS seq_no,
        SUM(score) OVER (PARTITION BY student_major, grade_status ORDER BY student_gpa ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_sum_3,
        student_gpa - LAG(student_gpa) OVER w_enrolled_at_month_by_student AS student_gpa_change,
        ROUND(PERCENT_RANK() OVER w_all_by_score, 3) AS score_percent_rank,
        FIRST_VALUE(score) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_major, grade_status ORDER BY student_gpa ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY student_major, student_cohort ORDER BY enrolled_at DESC, id) AS enrolled_at_dense_rank,
        ROUND(student_gpa - AVG(student_gpa) OVER w_enrolled_at_to_date, 2) AS student_gpa_vs_enrolled_at_to_date_avg,
        MIN(score) OVER (PARTITION BY TO_DATE(enrolled_at)) AS enrolled_at_to_date_min_score,
        LEAD(enrolled_at) OVER w_student_id_by_enrolled_at AS next_enrolled_at,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        SIZE(COLLECT_SET(student_major) OVER (PARTITION BY student_id)) AS distinct_student_major_in_student_id,
        ROUND(CUME_DIST() OVER (ORDER BY score), 3) AS score_cume_dist
    FROM joined_student
    WINDOW w_enrolled_at_month_by_student AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY student_gpa DESC, id), w_all_by_score AS (ORDER BY score, id), w_enrolled_at_to_date AS (PARTITION BY TO_DATE(enrolled_at)), w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at DESC NULLS FIRST, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(score_sum_3, 2) OVER (PARTITION BY student_major ORDER BY score_cume_dist ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_sum_3,
        ROUND(CUME_DIST() OVER (PARTITION BY student_major ORDER BY score_cume_dist ASC), 3) AS score_cume_dist_cume_dist,
        SUM(student_gpa_change) OVER (PARTITION BY distinct_student_major_in_student_id) AS distinct_student_major_in_student_id_sum_student,
        COUNT(*) OVER w_distinct_student_major_in_st AS rows_in_distinct_student_major_in_student_id,
        ROW_NUMBER() OVER (ORDER BY first_score DESC, id) AS rn,
        ROW_NUMBER() OVER w_student_major_by_score_cume_3 AS rn_2
    FROM windowed
    WINDOW w_distinct_student_major_in_st AS (PARTITION BY distinct_student_major_in_student_id), w_student_major_by_score_cume_3 AS (PARTITION BY student_major ORDER BY score_cume_dist ASC, id)
)
-- Final projection
SELECT
    id,
    course_id,
    enrolled_at,
    student_major,
    student_cohort,
    student_gpa,
    course_code,
    seq_no,
    score_sum_3,
    student_gpa_change,
    score_percent_rank,
    first_score,
    first_known_enrolled_at,
    student_gpa_vs_enrolled_at_to_date_avg,
    enrolled_at_to_date_min_score,
    next_enrolled_at,
    rows_in_student_id,
    score_cume_dist,
    second_score_sum_3,
    distinct_student_major_in_student_id_sum_student,
    rows_in_distinct_student_major_in_student_id,
    rn,
    rn_2,
    CONCAT_WS('|', student_cohort, grade_status) AS student_cohort_grade_status_label,
    GREATEST(score_sum_3, seq_no) AS greatest_score_sum_3_seq_no
FROM with_windows
WHERE score_cume_dist_cume_dist >= 0.2
ORDER BY distinct_student_major_in_student_id_sum_student DESC, rows_in_distinct_student_major_in_student_id
