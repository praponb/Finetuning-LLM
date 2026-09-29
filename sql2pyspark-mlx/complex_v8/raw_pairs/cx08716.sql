WITH valid_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        CASE WHEN gpa >= 3.2 THEN 'A' WHEN gpa >= 2.7 THEN 'B' ELSE 'C' END AS gpa_tier
    FROM students
    WHERE (gpa < 3.3 OR enrolled_on BETWEEN DATE '2024-02-28' AND DATE '2025-08-27')
)
SELECT id AS key_id, gpa AS value
FROM valid_students
WHERE UPPER(TRIM(student_name)) LIKE 'I%'
UNION ALL
SELECT id, gpa
FROM students
WHERE YEAR(enrolled_on) = 2024
  AND student_name IS NOT NULL
