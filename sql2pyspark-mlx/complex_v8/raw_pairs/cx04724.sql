WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE term = '2025T2'
),
latest AS (
    -- Keep the latest row per course_id, student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at DESC, id DESC) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.level AS course_level,
        c.course_code,
        c.credits AS course_credits,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM latest s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE UPPER(TRIM(c.course_code)) LIKE 'C%'
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND HOUR(e.enrolled_at) BETWEEN 10 AND 12
              AND e.term <> '2025T2'
        )
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.course_level,
        s.course_code,
        s.student_gpa,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status
    FROM screened s
    JOIN enrollments e ON e.course_id = s.course_id
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.course_level,
        s.course_code,
        s.student_gpa,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_student_id,
        s.enrollment_score,
        s.enrollment_grade_status,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa_2,
        st.enrolled_on AS student_enrolled_on
    FROM line_level_enrollments s
    JOIN students st ON st.id = s.student_id
)
SELECT
    enrollment_id,
    student_is_international,
    COALESCE(course_level, 'ALL') AS course_level,
    MAX(enrolled_at) AS last_enrolled_at,
    MIN(enrolled_at) AS first_enrolled_at
FROM with_parent_student
GROUP BY CUBE(enrollment_id, student_is_international, course_level)
ORDER BY enrollment_id NULLS LAST, student_is_international NULLS LAST, course_level NULLS LAST
