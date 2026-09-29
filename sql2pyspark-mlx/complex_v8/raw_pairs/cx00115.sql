WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE score <= 57.3
),
with_parent_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_len,
        c.level AS course_level,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa
    FROM recent_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        student_gpa,
        LEAD(enrolled_at) OVER w_student_id_by_student_gpa AS next_enrolled_at,
        DENSE_RANK() OVER (ORDER BY student_gpa DESC) AS student_gpa_dense_rank,
        DENSE_RANK() OVER w_student_id_by_student_gpa_ti AS student_gpa_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY student_gpa DESC), 3) AS student_gpa_percent_rank
    FROM with_parent_course
    WINDOW w_student_id_by_student_gpa AS (PARTITION BY student_id ORDER BY student_gpa DESC, id), w_student_id_by_student_gpa_ti AS (PARTITION BY student_id ORDER BY student_gpa DESC)
)
-- Subtotals with GROUPING SETS
SELECT
    term,
    student_gpa_dense_rank,
    GROUPING(term) AS is_total_term,
    PERCENTILE_APPROX(student_gpa_percent_rank, 0.5) AS median_student_gpa_percent_rank
FROM sequenced
GROUP BY GROUPING SETS ((term, student_gpa_dense_rank), (term))
