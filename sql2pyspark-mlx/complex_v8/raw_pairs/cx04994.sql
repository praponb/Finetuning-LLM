WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
    WHERE score < 62.5
),
combined_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_slug,
        s.grade_status_prefix,
        st.gpa AS student_gpa,
        st.student_name,
        st.is_international AS student_is_international,
        c.level AS course_level
    FROM scoped_enrollments s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM combined_details_student s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score < 62.5
        )
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at,
        grade_status_slug,
        grade_status_prefix,
        student_gpa,
        student_name,
        student_is_international,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM without_matches
),
enriched AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(enrolled_at_label), 1, 2) AS enrolled_at_label_prefix
    FROM labeled
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status_prefix,
        s.student_gpa,
        s.student_name,
        s.student_is_international,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id
    FROM enriched s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
enriched_2 AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.student_name,
        s.student_is_international,
        s.enrollment_id,
        s.enrollment_grade_status,
        s.enrollment_enrolled_at,
        s.enrollment_student_id,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.level AS course_level
    FROM line_level s
    JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    student_name,
    student_is_international,
    enrollment_id,
    enrollment_grade_status,
    enrollment_enrolled_at,
    course_credits,
    course_dept,
    course_level
FROM enriched_2
ORDER BY course_credits DESC NULLS FIRST, id, enrollment_id
LIMIT 20
