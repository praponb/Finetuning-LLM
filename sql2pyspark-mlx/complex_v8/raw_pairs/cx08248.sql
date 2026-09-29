WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        is_international,
        LOWER(TRIM(student_name)) AS student_name_clean,
        LAST_DAY(enrolled_on) AS enrolled_on_month_end,
        CONCAT(major, '#', CAST(id AS STRING)) AS major_ref
    FROM students
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.grade_status NOT IN ('FAIL', 'WITHDRAWN')
              AND e.term <> '2024T2'
        )
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_count,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS first_enrollment_enrolled_at
    FROM without_matches s
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM with_stats
    WHERE enrollment_count > (SELECT MIN(enrollment_count) FROM with_stats)
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_count_2
    FROM qualified s
)
-- Final projection
SELECT
    id,
    student_name,
    major,
    enrolled_on,
    gpa,
    is_international,
    enrolled_on_month_end,
    major_ref,
    enrollment_count,
    first_enrollment_enrolled_at,
    enrollment_count_2,
    SUBSTRING(TRIM(major), 1, 2) AS major_prefix
FROM summarized_rows
WHERE gpa > 2.1
ORDER BY enrollment_count_2, enrolled_on DESC NULLS FIRST
