WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND enrolled_at < TIMESTAMP '2025-02-18 08:00:00'
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id
    FROM cleaned s
    LEFT JOIN enrollments e ON e.student_id = s.student_id AND e.course_id <= 8
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrollment_id,
        s.enrollment_grade_status,
        s.enrollment_enrolled_at,
        s.enrollment_course_id,
        c.level AS course_level,
        c.course_code
    FROM detail_rows s
    LEFT JOIN courses c ON c.id = s.course_id
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(CASE WHEN term IS NOT NULL THEN 1 END) AS enrollment_term_count
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count
    FROM with_details_course s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
unique_rows AS (
    -- Keep the latest row per enrollment_course_id, course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrollment_id,
        enrollment_grade_status,
        enrollment_enrolled_at,
        enrollment_course_id,
        course_level,
        course_code,
        enrollment_term_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY enrollment_course_id, course_id ORDER BY enrollment_enrolled_at DESC, id DESC, enrollment_id DESC) AS row_num
        FROM with_stats
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM unique_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score > 5.9
              AND e.enrolled_at < TIMESTAMP '2025-02-18 08:00:00'
        )
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.*,
        st.is_international AS student_is_international,
        e.enrolled_at AS enrollment_enrolled_at_2
    FROM qualified s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
)
SELECT
    id,
    term,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_course_id,
    course_level,
    course_code,
    enrollment_term_count,
    student_is_international,
    enrollment_enrolled_at_2,
    ROUND((UNIX_TIMESTAMP(enrollment_enrolled_at) - UNIX_TIMESTAMP(enrollment_enrolled_at_2)) / 3600, 1) AS hours_enrollment_enrolled_at_2_to_enrollment_enr
FROM with_parent_student
ORDER BY enrollment_term_count, id, enrollment_id
LIMIT 10
