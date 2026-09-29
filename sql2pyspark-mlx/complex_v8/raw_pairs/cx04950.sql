WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE score <= 66.0
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort,
        c.level AS course_level,
        c.dept AS course_dept
    FROM qualified s
    JOIN students st ON st.id = s.student_id AND st.is_international = TRUE
    LEFT JOIN courses c ON c.id = s.course_id
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM combined_details_student s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status NOT IN ('FAIL', 'PASS')
        )
      AND s.score BETWEEN 30.0 AND 73.7
),
filtered_2 AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at < TIMESTAMP '2025-03-08 10:00:00'
        )
      AND s.score <= 59.8
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    enrolled_at,
    student_gpa,
    student_cohort,
    course_level,
    course_dept,
    GREATEST(score, student_gpa) AS greatest_score_student_gpa
FROM filtered_2
