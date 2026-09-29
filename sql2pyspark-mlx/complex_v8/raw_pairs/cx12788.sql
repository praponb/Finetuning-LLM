WITH cleaned AS (
    -- Filter students
    SELECT *
    FROM students
    WHERE major IN ('Bio', 'Biz', 'Math')
      AND gpa BETWEEN 2.1 AND 3.4
)
SELECT id
FROM cleaned
INTERSECT
SELECT id
FROM students
ORDER BY id DESC
