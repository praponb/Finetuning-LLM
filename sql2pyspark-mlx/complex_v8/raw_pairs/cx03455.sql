WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ROUND(score * 1.5, 2) AS score_adj
    FROM enrollments
    WHERE score >= 32.9
),
with_parent AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.score_adj,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM cleaned s
    JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    student_gpa,
    ROUND(score / NULLIF(score_adj, 0), 3) AS score_per_score_adj
FROM with_parent
ORDER BY student_gpa DESC, term
