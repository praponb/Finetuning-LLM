WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-24'
      AND term = '2024T1'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_adj,
        enrolled_at_quarter,
        COUNT(*) OVER (PARTITION BY grade_status, term) AS rows_in_grade_status_term,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, term), 2) AS grade_status_term_avg_score,
        NTILE(10) OVER (PARTITION BY grade_status, course_id ORDER BY enrolled_at_quarter ASC, id) AS enrolled_at_quarter_decile,
        LAG(score_adj) OVER w_course_id_student_id_by_enro AS prev_score_adj
    FROM scoped
    WINDOW w_course_id_student_id_by_enro AS (PARTITION BY course_id, student_id ORDER BY enrolled_at DESC, id)
),
enriched_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_adj,
        s.enrolled_at_quarter,
        s.rows_in_grade_status_term,
        s.grade_status_term_avg_score,
        s.prev_score_adj,
        st.gpa AS student_gpa,
        st.enrolled_on AS student_enrolled_on,
        st.is_international AS student_is_international
    FROM windowed s
    JOIN students st ON st.id = s.student_id
    WHERE st.is_international = FALSE
)
SELECT
    course_id,
    student_id,
    ROUND(SUM(score_adj) / NULLIF(SUM(student_gpa), 0), 3) AS score_adj_to_student_gpa_ratio
FROM enriched_student
GROUP BY CUBE(course_id, student_id)
ORDER BY course_id NULLS LAST, student_id NULLS LAST
