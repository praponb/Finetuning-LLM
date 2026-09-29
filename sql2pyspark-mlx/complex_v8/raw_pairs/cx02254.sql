WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE term = '2025T1'
      AND course_id IS NOT NULL
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.dept AS course_dept,
        c.level AS course_level,
        c.course_code,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort
    FROM prepared_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
student_id_stats AS (
    -- Median score per student_id
    SELECT student_id, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM combined_details_course
    GROUP BY student_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM combined_details_course s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score >= g.median_score
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_grade_status_label,
        s.score_filled,
        s.course_dept,
        s.course_level,
        s.course_code,
        s.student_enrolled_on,
        s.student_cohort,
        s.median_score,
        st.major AS student_major,
        st.gpa AS student_gpa
    FROM outliers s
    JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    term_grade_status_label,
    score_filled,
    course_dept,
    course_level,
    student_enrolled_on,
    median_score,
    student_major,
    student_gpa
FROM enriched
ORDER BY student_enrolled_on, student_major DESC, id
LIMIT 20
