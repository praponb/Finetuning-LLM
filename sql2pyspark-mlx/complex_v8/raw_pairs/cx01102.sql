WITH filtered_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        UPPER(TRIM(cohort)) AS cohort_clean
    FROM students
    WHERE is_international = TRUE
      AND enrolled_on < DATE '2025-07-03'
)
SELECT id, enrolled_on AS day
FROM filtered_students
WHERE enrolled_on BETWEEN DATE '2024-02-28' AND DATE '2025-08-27'
UNION ALL
SELECT id, enrolled_on AS day
FROM students
WHERE gpa BETWEEN 2.7 AND 3.2
  AND cohort NOT IN ('C2021', 'C2022')
