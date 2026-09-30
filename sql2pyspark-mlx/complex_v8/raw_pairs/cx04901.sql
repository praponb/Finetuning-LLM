WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN', 'FAIL')
),
joined_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_term_label,
        s.term_len,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international,
        st.enrolled_on AS student_enrolled_on,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM recent s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        course_credits - LAG(course_credits) OVER w_grade_status_by_term_len AS course_credits_change,
        MAX(course_credits) OVER (PARTITION BY course_credits) AS course_credits_max_course_credits,
        DENSE_RANK() OVER w_all_by_student_gpa_ties AS student_gpa_dense_rank
    FROM joined_student
    WINDOW w_grade_status_by_term_len AS (PARTITION BY grade_status ORDER BY term_len, id), w_all_by_student_gpa_ties AS (ORDER BY student_gpa ASC)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        grade_status_term_label,
        term_len,
        student_gpa,
        student_is_international,
        student_enrolled_on,
        course_dept,
        course_credits,
        course_credits_change,
        course_credits_max_course_credits,
        student_gpa_dense_rank,
        RANK() OVER w_all_by_course_credits_change AS course_credits_change_rank,
        RANK() OVER (ORDER BY student_enrolled_on DESC NULLS FIRST) AS student_enrolled_on_rank
    FROM with_trends
    WINDOW w_all_by_course_credits_change AS (ORDER BY course_credits_change DESC, id)
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.grade_status_term_label,
        s.term_len,
        s.student_is_international,
        s.student_enrolled_on,
        s.course_dept,
        s.course_credits,
        s.course_credits_max_course_credits,
        s.student_gpa_dense_rank,
        s.course_credits_change_rank,
        s.student_enrolled_on_rank,
        st.gpa AS student_gpa_2,
        st.cohort AS student_cohort
    FROM with_windows s
    JOIN students st ON st.id = s.student_id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        enrolled_at,
        term_len,
        student_is_international,
        student_enrolled_on,
        course_dept,
        student_gpa_dense_rank,
        course_credits_change_rank,
        student_enrolled_on_rank,
        student_gpa_2,
        student_cohort,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day,
        LOWER(TRIM(course_dept)) AS course_dept_clean
    FROM enriched
),
student_cohort_stats AS (
    -- Avg student_gpa_dense_rank per student_cohort
    SELECT student_cohort, AVG(student_gpa_dense_rank) AS avg_student_gpa_dense_rank
    FROM annotated
    GROUP BY student_cohort
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_student_gpa_dense_rank,
        ROUND(s.student_gpa_dense_rank - g.avg_student_gpa_dense_rank, 2) AS student_gpa_dense_rank_minus_avg
    FROM annotated s
    JOIN student_cohort_stats g ON g.student_cohort = s.student_cohort
    WHERE s.student_gpa_dense_rank > g.avg_student_gpa_dense_rank
)
SELECT
    id,
    student_id,
    term_len,
    student_enrolled_on,
    course_dept,
    student_gpa_dense_rank,
    course_credits_change_rank,
    student_enrolled_on_rank,
    student_cohort,
    enrolled_at_day,
    course_dept_clean,
    avg_student_gpa_dense_rank,
    student_gpa_dense_rank_minus_avg,
    DATEDIFF(DATE '2026-01-01', student_enrolled_on) AS days_since_student_enrolled_on,
    ROUND(student_gpa_2 * 0.25, 2) AS student_gpa_2_adj
FROM outliers
WHERE course_credits_change_rank > 2
ORDER BY course_dept
