WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2024T2', '2025T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE (score < 85.8 OR course_id IS NOT NULL)
),
deduped AS (
    -- Keep the earliest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_group,
        term_ref,
        enrolled_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY enrolled_at, id) AS recency_rank
        FROM prepared_enrollments
    ) ranked
    WHERE recency_rank = 1
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status IS NOT NULL
        )
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.term_ref,
        s.enrolled_at_label,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status
    FROM without_matches s
    JOIN enrollments e ON e.course_id = s.course_id AND HOUR(e.enrolled_at) BETWEEN 8 AND 14
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(enrollment_enrolled_at) - UNIX_TIMESTAMP(enrolled_at)) / 60, 1) AS minutes_enrolled_at_to_enrollment_enrolled_at
    FROM with_children_enrollments
    WHERE course_id >= 7
),
latest_per_key AS (
    -- Keep the earliest row per student_id, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_ref,
        enrolled_at_label,
        enrollment_id,
        enrollment_student_id,
        enrollment_term,
        enrollment_enrolled_at,
        enrollment_grade_status,
        minutes_enrolled_at_to_enrollment_enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, term ORDER BY enrolled_at, id, enrollment_id) AS rn
        FROM derived
    ) ranked
    WHERE rn = 1
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, c.course_code, c.dept AS course_dept
    FROM latest_per_key s
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    grade_status,
    enrolled_at,
    term_ref,
    enrolled_at_label,
    enrollment_id,
    enrollment_student_id,
    enrollment_term,
    enrollment_enrolled_at,
    minutes_enrolled_at_to_enrollment_enrolled_at,
    course_code,
    course_dept,
    DATEDIFF(DATE '2025-12-31', enrollment_enrolled_at) AS days_since_enrollment_enrolled_at
FROM with_details
WHERE minutes_enrolled_at_to_enrollment_enrolled_at >= -1200.0
ORDER BY enrolled_at NULLS LAST
