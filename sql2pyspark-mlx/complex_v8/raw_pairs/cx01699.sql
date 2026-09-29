WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        LENGTH(TRIM(cohort)) AS cohort_len
    FROM students
    WHERE student_name IS NOT NULL
)
SELECT major AS label, cohort AS name
FROM base
WHERE YEAR(enrolled_on) = 2025
UNION
SELECT term, grade_status
FROM enrollments
WHERE term = '2025T2'
ORDER BY label DESC, name
