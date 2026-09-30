WITH prepared_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        QUARTER(enrolled_on) AS enrolled_on_quarter
    FROM students
    WHERE YEAR(enrolled_on) = 2025
      AND LOWER(student_name) LIKE '%s %'
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS last_enrollment_enrolled_at
    FROM prepared_students s
),
tagged AS (
    -- Label and score rows
    SELECT *, ADD_MONTHS(enrolled_on, 12) AS enrolled_on_plus_12m
    FROM with_counts
),
enriched AS (
    -- Label and score rows
    SELECT *, gpa - enrolled_on_quarter AS gpa_minus_enrolled_on_quarter
    FROM tagged
    WHERE major = 'Bio'
)
-- Subtotals with GROUPING SETS
SELECT
    cohort,
    enrolled_on_quarter,
    ROUND(SUM(gpa), 2) AS total_gpa
FROM enriched
GROUP BY GROUPING SETS ((cohort, enrolled_on_quarter), (cohort), ())
ORDER BY cohort NULLS LAST, enrolled_on_quarter NULLS LAST
