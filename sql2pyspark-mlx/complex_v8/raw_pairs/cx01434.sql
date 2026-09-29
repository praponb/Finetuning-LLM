WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE student_id >= 3
),
joined AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.term_slug,
        s.enrolled_at_date,
        c.dept AS course_dept,
        c.credits AS course_credits,
        st.gpa AS student_gpa
    FROM recent s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.dept <> 'Math'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_term_student_id_by_enrolled AS seq_no,
        COUNT(*) OVER w_course_id AS rows_in_course_id,
        MIN(student_gpa) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_date), course_credits) AS enrolled_at_date_month_course_credits_min_studen,
        ROUND(course_credits - AVG(course_credits) OVER w_course_id, 2) AS course_credits_vs_course_id_avg,
        SIZE(COLLECT_SET(term) OVER w_enrolled_at_date_month_cours) AS distinct_term_in_enrolled_at_date_month_course_c,
        NTH_VALUE(student_gpa, 2) OVER (ORDER BY enrolled_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_gpa,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_date), course_credits), 2) AS student_gpa_vs_enrolled_at_date_month_course_cre,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id)) AS distinct_term_in_course_id,
        LAG(student_gpa) OVER w_term_student_id_by_enrolled AS prev_student_gpa,
        DENSE_RANK() OVER (ORDER BY course_credits DESC NULLS FIRST, id) AS course_credits_dense_rank
    FROM joined
    WINDOW w_term_student_id_by_enrolled AS (PARTITION BY term, student_id ORDER BY enrolled_at_date, id), w_course_id AS (PARTITION BY course_id), w_enrolled_at_date_month_cours AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_date), course_credits)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        term_slug,
        enrolled_at_date,
        course_credits,
        rows_in_course_id,
        enrolled_at_date_month_course_credits_min_studen,
        course_credits_vs_course_id_avg,
        distinct_term_in_enrolled_at_date_month_course_c,
        second_student_gpa,
        student_gpa_vs_enrolled_at_date_month_course_cre,
        distinct_term_in_course_id,
        prev_student_gpa,
        course_credits_dense_rank,
        ROUND(AVG(seq_no) OVER (ORDER BY course_credits ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS seq_no_avg_6_prior,
        ROW_NUMBER() OVER (ORDER BY course_credits ASC, id) AS course_credits_seq,
        FIRST_VALUE(second_student_gpa) IGNORE NULLS OVER (PARTITION BY student_id, course_dept ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_student_gpa,
        SUM(second_student_gpa) OVER (PARTITION BY student_id, course_dept ORDER BY course_credits DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS second_student_gpa_sum_3,
        SUM(student_gpa) OVER (PARTITION BY course_dept ORDER BY distinct_term_in_course_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_student_gpa,
        LEAD(distinct_term_in_course_id, 1, 0) OVER (ORDER BY course_credits ASC, id) AS next_distinct_term_in_course_id
    FROM ranked
)
SELECT
    id,
    term_slug,
    enrolled_at_date,
    course_credits,
    rows_in_course_id,
    enrolled_at_date_month_course_credits_min_studen,
    course_credits_vs_course_id_avg,
    second_student_gpa,
    student_gpa_vs_enrolled_at_date_month_course_cre,
    distinct_term_in_course_id,
    course_credits_dense_rank,
    course_credits_seq,
    first_known_second_student_gpa,
    second_student_gpa_sum_3,
    running_student_gpa
FROM metrics
WHERE course_id <= 6
ORDER BY course_credits
