WITH cleaned AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        is_international,
        CONCAT(major, '#', CAST(id AS STRING)) AS major_ref
    FROM students
    WHERE is_international = FALSE
      AND gpa >= 2.0
)
SELECT 'student' AS src, major_ref AS label, enrolled_on AS event_date
FROM cleaned
UNION
SELECT 'student_2' AS src, student_name AS label, enrolled_on AS event_date
FROM students
WHERE gpa > 2.7
