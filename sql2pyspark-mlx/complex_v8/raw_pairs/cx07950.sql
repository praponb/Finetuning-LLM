WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE student_id <= 18
      AND grade_status IS NOT NULL
),
combined_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.enrolled_at_quarter,
        st.student_name
    FROM scoped_enrollments s
    JOIN students st ON st.id = s.student_id
),
student_id_stats AS (
    -- Median score per student_id
    SELECT student_id, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM combined_details_student
    GROUP BY student_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM combined_details_student s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score >= g.median_score
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at_quarter,
        s.student_name,
        s.score_minus_median,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM outliers s
    JOIN enrollments e ON e.course_id = s.course_id AND e.student_id IS NOT NULL
),
with_details AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at_quarter,
        s.student_name,
        s.score_minus_median,
        s.enrollment_id,
        s.enrollment_student_id,
        s.enrollment_grade_status,
        s.enrollment_score,
        c.dept AS course_dept,
        c.level AS course_level,
        c.course_code,
        st.gpa AS student_gpa,
        st.student_name AS student_student_name
    FROM with_children s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.enrollment_student_id
)
-- Subtotals with ROLLUP
SELECT
    course_level,
    enrollment_student_id,
    enrollment_id,
    COUNT(DISTINCT course_level) AS distinct_course_levels
FROM with_details
GROUP BY ROLLUP(course_level, enrollment_student_id, enrollment_id)
ORDER BY course_level NULLS LAST, enrollment_student_id NULLS LAST, enrollment_id NULLS LAST
