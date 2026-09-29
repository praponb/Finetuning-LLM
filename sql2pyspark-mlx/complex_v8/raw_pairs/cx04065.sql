WITH prepared_enrollments AS (
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
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international,
        st.student_name,
        c.course_code,
        c.dept AS course_dept
    FROM prepared_enrollments s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
    WHERE UPPER(TRIM(st.student_name)) LIKE 'D%'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term, course_dept ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY student_cohort, grade_status ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROW_NUMBER() OVER w_student_cohort_grade_status_2 AS enrolled_at_seq,
        DENSE_RANK() OVER (PARTITION BY student_is_international ORDER BY enrolled_at, id) AS enrolled_at_dense_rank,
        DENSE_RANK() OVER (ORDER BY enrolled_at DESC NULLS FIRST) AS enrolled_at_dense_rank_2,
        LEAD(enrolled_at) OVER (PARTITION BY student_is_international ORDER BY enrolled_at, id) AS next_enrolled_at,
        RANK() OVER (PARTITION BY term, course_dept ORDER BY enrolled_at ASC, id) AS enrolled_at_rank,
        NTILE(4) OVER w_student_is_international_by AS enrolled_at_quartile,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_cohort, grade_status ORDER BY enrolled_at ASC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROW_NUMBER() OVER w_term_course_dept_by_enrolled_2 AS enrolled_at_seq_2,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY term, course_dept ORDER BY enrolled_at ASC, id))) / 60, 1) AS minutes_since_prev_enrolled_at_2,
        NTILE(10) OVER (PARTITION BY term, course_dept ORDER BY enrolled_at ASC, id) AS enrolled_at_decile,
        LAG(enrolled_at) OVER (PARTITION BY student_is_international ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM enriched
    WINDOW w_student_cohort_grade_status_2 AS (PARTITION BY student_cohort, grade_status ORDER BY enrolled_at ASC, id), w_student_is_international_by AS (PARTITION BY student_is_international ORDER BY enrolled_at, id), w_term_course_dept_by_enrolled_2 AS (PARTITION BY term, course_dept ORDER BY enrolled_at ASC, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        student_is_international,
        student_name,
        course_code,
        course_dept,
        second_enrolled_at,
        enrolled_at_dense_rank,
        enrolled_at_dense_rank_2,
        next_enrolled_at,
        enrolled_at_rank,
        enrolled_at_quartile,
        second_enrolled_at_2,
        minutes_since_prev_enrolled_at,
        enrolled_at_seq_2,
        minutes_since_prev_enrolled_at_2,
        enrolled_at_decile,
        prev_enrolled_at,
        CASE WHEN grade_status IN ('WITHDRAWN', 'PASS') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM with_trends
)
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    student_is_international,
    student_name,
    course_code,
    course_dept,
    enrolled_at_dense_rank,
    enrolled_at_quartile,
    second_enrolled_at_2,
    minutes_since_prev_enrolled_at_2,
    enrolled_at_decile,
    grade_status_group
FROM annotated
ORDER BY course_code, minutes_since_prev_enrolled_at_2
