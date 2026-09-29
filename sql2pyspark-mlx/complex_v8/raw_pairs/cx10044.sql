WITH filtered AS (
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
    WHERE HOUR(enrolled_at) BETWEEN 6 AND 8
      AND grade_status <> 'FAIL'
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
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term
    FROM filtered s
    JOIN enrollments e ON e.course_id = s.course_id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrollment_id,
        enrollment_student_id,
        enrollment_term,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM line_level_enrollments
    WHERE term IS NOT NULL
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM annotated s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
        )
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.enrollment_student_id) AS first_enrollment_enrolled_at
    FROM without_matches s
),
filtered_2 AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND MONTH(e.enrolled_at) IN (1, 2)
        )
)
SELECT
    id,
    student_id,
    course_id,
    term,
    enrollment_id,
    enrollment_student_id,
    enrollment_term,
    grade_status_len,
    LENGTH(TRIM(grade_status)) AS grade_status_len_2
FROM filtered_2
WHERE HOUR(first_enrollment_enrolled_at) BETWEEN 10 AND 15
ORDER BY id
