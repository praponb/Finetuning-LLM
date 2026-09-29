WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score BETWEEN 8.7 AND 73.0
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        st.is_international AS student_is_international,
        st.student_name,
        c.course_code,
        c.credits AS course_credits
    FROM cleaned s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        student_is_international,
        student_name,
        course_credits,
        SUM(score) OVER w_grade_status_term AS grade_status_term_sum_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY course_credits DESC), 3) AS course_credits_percent_rank,
        LAST_VALUE(course_credits) OVER (ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_course_credits
    FROM joined
    WINDOW w_grade_status_term AS (PARTITION BY grade_status, term)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        IF(student_is_international, 'yes', 'no') AS student_is_international_label
    FROM sequenced
    WHERE student_is_international = TRUE
)
SELECT
    last_course_credits,
    COALESCE(term, 'ALL') AS term,
    course_credits,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
    ROUND(AVG(grade_status_term_sum_score), 2) AS avg_grade_status_term_sum_score,
    ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list
FROM annotated
GROUP BY CUBE(last_course_credits, term, course_credits)
ORDER BY last_course_credits NULLS LAST, term NULLS LAST, course_credits NULLS LAST
