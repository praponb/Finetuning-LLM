WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE (course_id <= 4 OR grade_status <> 'WITHDRAWN')
),
derived AS (
    -- Label and score rows
    SELECT *, CONCAT_WS(': ', grade_status, term) AS grade_status_term_label
    FROM scoped
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.grade_status_term_label,
        st.gpa AS student_gpa
    FROM derived s
    JOIN students st ON st.id = s.student_id
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_parent_student s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id IS NOT NULL
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        CASE WHEN term IN ('2025T2', '2024T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group
    FROM screened
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(score) AS max_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.max_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM annotated s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.grade_status_term_label,
        s.student_gpa,
        s.grade_status_or_unknown,
        s.term_group,
        s.max_enrollment_score,
        s.enrollment_count,
        st.gpa AS student_gpa_2,
        st.major AS student_major,
        c.level AS course_level
    FROM summarized_rows s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    student_gpa,
    grade_status_or_unknown,
    term_group,
    student_gpa_2,
    student_major,
    course_level
FROM joined
ORDER BY student_gpa_2 NULLS LAST, grade_status DESC
