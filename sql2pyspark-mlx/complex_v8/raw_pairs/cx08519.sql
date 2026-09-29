WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        CASE WHEN score >= 57.3 THEN 'large' WHEN score >= 20.2 THEN 'medium' ELSE 'small' END AS score_tier,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE student_id IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(score) OVER (PARTITION BY term_or_unknown, student_id ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        LEAD(enrolled_at) OVER (PARTITION BY term_or_unknown, student_id ORDER BY score DESC, id) AS next_enrolled_at,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        LAG(enrolled_at, 3) OVER (PARTITION BY term_or_unknown, student_id ORDER BY score DESC, id) AS prev3_enrolled_at,
        NTILE(4) OVER (PARTITION BY term_or_unknown, student_id ORDER BY score DESC, id) AS score_quartile
    FROM filtered_enrollments
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT s.*, c.credits AS course_credits
    FROM ranked s
    LEFT JOIN courses c ON c.id = s.course_id
),
with_parent AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.term_or_unknown,
        s.score_tier,
        s.term_grade_status_label,
        s.score_avg_3_prior,
        s.next_enrolled_at,
        s.score_share_of_student_id,
        s.prev3_enrolled_at,
        s.score_quartile,
        st.student_name,
        st.gpa AS student_gpa,
        st.major AS student_major
    FROM with_parent_course s
    JOIN students st ON st.id = s.student_id
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.term_or_unknown,
        s.score_tier,
        s.term_grade_status_label,
        s.score_avg_3_prior,
        s.score_share_of_student_id,
        s.prev3_enrolled_at,
        s.score_quartile,
        s.student_name,
        s.student_gpa,
        s.student_major,
        c.credits AS course_credits,
        c.course_code
    FROM with_parent s
    LEFT JOIN courses c ON c.id = s.course_id
),
with_parent_course_2 AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.term_or_unknown,
        s.score_tier,
        s.term_grade_status_label,
        s.score_share_of_student_id,
        s.prev3_enrolled_at,
        s.score_quartile,
        s.student_gpa,
        s.student_major,
        s.course_credits,
        s.course_code,
        c.dept AS course_dept,
        st.gpa AS student_gpa_2,
        st.student_name AS student_student_name
    FROM with_details_course s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
)
-- Subtotals with CUBE
SELECT
    student_major,
    course_dept,
    course_credits,
    SUM(CASE WHEN UPPER(TRIM(course_code)) LIKE 'A%' THEN student_gpa_2 ELSE 0 END) AS student_gpa_2_upper_trim_course_code_like,
    COUNT(DISTINCT student_id) AS distinct_student_ids,
    COUNT(DISTINCT term_or_unknown) AS distinct_term_or_unknowns
FROM with_parent_course_2
GROUP BY CUBE(student_major, course_dept, course_credits)
ORDER BY student_major NULLS LAST, course_dept NULLS LAST, course_credits NULLS LAST
