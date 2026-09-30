WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE student_id >= 19
      AND course_id IS NOT NULL
),
with_parent AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at_quarter,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international,
        c.credits AS course_credits
    FROM base_enrollments s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    course_id,
    term,
    grade_status,
    enrolled_at_quarter,
    student_gpa,
    course_credits,
    CASE WHEN term IN ('2024T1', '2025T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group,
    ABS(course_credits - 3) AS course_credits_distance
FROM with_parent
ORDER BY student_gpa NULLS LAST, course_credits
