WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM enrollments
    WHERE student_id >= 15
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id
    FROM scoped_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM with_children
    WHERE enrollment_id IS NOT NULL
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.first_enrollment_enrolled_at
    FROM enriched s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status IN ('PASS', 'INCOMPLETE', 'WITHDRAWN')
        )
      AND s.enrolled_at >= TIMESTAMP '2025-02-06 11:00:00'
),
unique_rows AS (
    -- Keep the latest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrollment_id,
        enrollment_term,
        enrollment_student_id,
        enrolled_at_label,
        grade_status_prefix,
        term_slug,
        last_enrollment_enrolled_at,
        enrollment_count,
        first_enrollment_enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY enrolled_at DESC, id DESC, enrollment_id DESC) AS recency_rank
        FROM without_matches
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    enrollment_id,
    grade_status_prefix,
    term_slug,
    last_enrollment_enrolled_at,
    enrollment_count,
    DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year,
    COALESCE(enrollment_count, 0) AS enrollment_count_filled
FROM unique_rows
ORDER BY term_slug, course_id DESC
