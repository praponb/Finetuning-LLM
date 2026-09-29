WITH cleaned_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        CASE WHEN gpa >= 3.4 THEN 'HIGH' WHEN gpa >= 2.7 THEN 'MEDIUM' ELSE 'LOW' END AS gpa_tier
    FROM students
    WHERE is_international = TRUE
),
stacked AS (
    -- Stack the branches
    SELECT student_name, major AS category, enrolled_on
    FROM cleaned_students
    UNION ALL
    SELECT cohort, major, enrolled_on
    FROM students
)
SELECT student_name, COUNT(*) AS n_rows, MAX(enrolled_on) AS latest_enrolled_on
FROM stacked
GROUP BY student_name
ORDER BY student_name
