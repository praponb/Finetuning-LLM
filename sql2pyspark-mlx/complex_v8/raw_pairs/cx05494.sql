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
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 3)
      AND course_id IS NOT NULL
),
enriched AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.days_since_enrolled_at,
        c.level AS course_level,
        c.dept AS course_dept
    FROM scoped s
    LEFT JOIN courses c ON c.id = s.course_id AND c.dept <> 'CS'
),
with_parent_course AS (
    -- Attach course attributes
    SELECT
        s.*,
        c.credits AS course_credits,
        st.enrolled_on AS student_enrolled_on,
        st.gpa AS student_gpa
    FROM enriched s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.credits <= 3
)
SELECT id, metric_name, amount
FROM (
    SELECT id, score, student_gpa
    FROM with_parent_course
) src
UNPIVOT (
    amount FOR metric_name IN (score, student_gpa)
) unp
