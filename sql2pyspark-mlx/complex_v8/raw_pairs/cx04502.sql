WITH combined AS (
    -- Stack the branches
    SELECT major, enrolled_on
    FROM students
    WHERE is_international = FALSE
      AND LENGTH(TRIM(student_name)) >= 6
    UNION
    SELECT student_name, enrolled_on
    FROM students
)
SELECT major, COUNT(*) AS n_rows, MAX(enrolled_on) AS latest_enrolled_on
FROM combined
GROUP BY major
ORDER BY major
