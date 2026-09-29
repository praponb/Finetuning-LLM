WITH scoped AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        gpa,
        DATE_FORMAT(enrolled_on, 'MMM yyyy') AS enrolled_on_label
    FROM students
    WHERE major <> 'CS'
      AND YEAR(enrolled_on) = 2024
)
SELECT id
FROM scoped
WHERE cohort = 'C2024'
  AND enrolled_on_label IS NOT NULL
EXCEPT ALL
SELECT id
FROM courses
ORDER BY id DESC
