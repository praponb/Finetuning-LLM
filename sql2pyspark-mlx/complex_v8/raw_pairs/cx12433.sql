WITH filtered_students AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM students
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        LOWER(REGEXP_REPLACE(TRIM(major), ' ', '_')) AS major_slug,
        MONTH(enrolled_on) AS enrolled_on_month,
        CASE WHEN gpa >= 3.2 THEN 'large' WHEN gpa >= 2.7 THEN 'medium' ELSE 'small' END AS gpa_tier
    FROM filtered_students
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
        )
)
SELECT *
FROM (
    SELECT enrolled_on_month, enrolled_on, cohort, gpa
    FROM screened
) src
PIVOT (
    SUM(gpa)
    FOR cohort IN ('C2024', 'C2023', 'C2021')
)
