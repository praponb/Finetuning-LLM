WITH prepared AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        DATE_TRUNC('QUARTER', enrolled_on) AS enrolled_on_quarter,
        MONTH(enrolled_on) AS enrolled_on_month,
        CONCAT_WS('-', cohort, student_name) AS cohort_student_name_label
    FROM students
    WHERE gpa > 2.0
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.grade_status <> 'INCOMPLETE'
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(enrolled_on_month / NULLIF(gpa, 0), 3) AS enrolled_on_month_per_gpa,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug,
        LOWER(TRIM(major)) AS major_clean
    FROM filtered
)
SELECT id, measure, metric_value
FROM (
    SELECT id, enrolled_on_month_per_gpa, gpa
    FROM labeled
) src
UNPIVOT (
    metric_value FOR measure IN (enrolled_on_month_per_gpa, gpa)
) unp
ORDER BY id, measure
