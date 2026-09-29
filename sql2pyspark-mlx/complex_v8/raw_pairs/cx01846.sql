WITH cleaned_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        enrolled_on,
        gpa,
        is_international,
        ROUND(gpa * 0.9, 2) AS gpa_adj,
        DATE_ADD(enrolled_on, 30) AS enrolled_on_plus_30d
    FROM students
    WHERE major = 'Art'
      AND enrolled_on IS NOT NULL
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned_students
    WHERE gpa_adj > (SELECT AVG(gpa_adj) FROM cleaned_students)
)
SELECT
    id,
    student_name,
    enrolled_on,
    gpa,
    is_international,
    gpa_adj,
    enrolled_on_plus_30d
FROM qualified
WHERE is_international = TRUE
ORDER BY enrolled_on_plus_30d, gpa_adj
