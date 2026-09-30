WITH stacked AS (
    -- Stack the branches
    SELECT id, enrolled_on
    FROM students
    WHERE enrolled_on >= DATE '2024-04-05'
      AND UPPER(TRIM(student_name)) LIKE 'B%'
    UNION
    SELECT id, enrolled_on
    FROM students
    WHERE LENGTH(TRIM(student_name)) >= 5
      AND NOT is_international
)
SELECT id, COUNT(*) AS n_rows, MAX(enrolled_on) AS latest_enrolled_on
FROM stacked
GROUP BY id
ORDER BY id
