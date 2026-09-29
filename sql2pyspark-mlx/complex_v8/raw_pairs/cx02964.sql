WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        COALESCE(gpa, 0.0) AS gpa_filled,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug,
        DATE_ADD(enrolled_on, 30) AS enrolled_on_plus_30d
    FROM students
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.cohort,
        s.major,
        s.gpa_filled,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM cleaned s
    JOIN enrollments e ON e.student_id = s.id
),
latest_per_key AS (
    -- Keep the latest row per enrollment_course_id, major
    SELECT
        id,
        cohort,
        major,
        gpa_filled,
        enrollment_id,
        enrollment_course_id,
        enrollment_enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY enrollment_course_id, major ORDER BY gpa_filled DESC, id DESC, enrollment_id DESC) AS row_num
        FROM line_level
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM latest_per_key s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.enrollment_course_id
              AND e.score <= 77.0
              AND e.student_id >= 15
        )
)
SELECT
    enrollment_id,
    COALESCE(cohort, 'ALL') AS cohort,
    COALESCE(major, 'ALL') AS major,
    MAX(gpa_filled) AS max_gpa_filled,
    COUNT(DISTINCT id) AS distinct_ids
FROM filtered
GROUP BY CUBE(enrollment_id, cohort, major)
ORDER BY enrollment_id NULLS LAST, cohort NULLS LAST, major NULLS LAST
