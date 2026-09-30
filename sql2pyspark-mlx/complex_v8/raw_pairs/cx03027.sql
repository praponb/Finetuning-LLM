WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        CASE WHEN gpa >= 3.3 THEN 'HIGH' WHEN gpa >= 2.1 THEN 'MEDIUM' ELSE 'LOW' END AS gpa_tier
    FROM students
    WHERE cohort NOT IN ('C2022', 'C2023')
)
SELECT gpa_tier AS category, gpa
FROM scoped
WHERE gpa <= 3.4
UNION ALL
SELECT major, gpa
FROM students
ORDER BY gpa DESC, category
