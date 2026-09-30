WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score <= 57.6
      AND MONTH(enrolled_at) IN (2, 3)
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM scoped s
    JOIN students st ON st.id = s.student_id
    WHERE st.gpa >= 2.0
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_parent_student s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
)
-- Subtotals with CUBE
SELECT
    term,
    student_id,
    student_is_international,
    GROUPING(term) AS is_total_term,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list
FROM screened
GROUP BY CUBE(term, student_id, student_is_international)
