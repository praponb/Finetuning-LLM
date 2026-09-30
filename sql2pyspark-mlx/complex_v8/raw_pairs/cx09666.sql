WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ABS(score - 57.6) AS score_distance
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
),
with_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        st.enrolled_on AS student_enrolled_on
    FROM prepared_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
with_parent_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.student_enrolled_on,
        st.is_international AS student_is_international,
        st.cohort AS student_cohort,
        st.student_name
    FROM with_details s
    JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    student_enrolled_on,
    student_is_international,
    student_cohort,
    student_name,
    COALESCE(term, 'UNKNOWN') AS term_or_unknown,
    DATE_TRUNC('QUARTER', student_enrolled_on) AS student_enrolled_on_quarter
FROM with_parent_student
WHERE student_name IS NOT NULL
ORDER BY term NULLS LAST
