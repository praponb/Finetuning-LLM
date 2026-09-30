WITH scoped AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE gpa <= 3.4
      AND is_international = TRUE
)
SELECT id, cohort
FROM scoped
UNION ALL
SELECT id, student_name
FROM students
WHERE cohort NOT IN ('C2024', 'C2021')
  AND gpa < 3.4
UNION ALL
SELECT id, cohort
FROM students
WHERE LENGTH(TRIM(student_name)) >= 4
ORDER BY cohort DESC
