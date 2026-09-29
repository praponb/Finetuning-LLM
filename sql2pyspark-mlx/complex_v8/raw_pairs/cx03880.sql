WITH valid_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE gpa BETWEEN 1.7 AND 3.4
),
all_events AS (
    -- Stack the branches
    SELECT cohort AS name, enrolled_on AS day, id
    FROM valid_students
    WHERE cohort IN ('C2021', 'C2022')
      AND UPPER(TRIM(student_name)) LIKE 'L%'
    UNION ALL
    SELECT major AS name, enrolled_on AS day, id
    FROM students
)
SELECT name, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(day) AS latest_day
FROM all_events
GROUP BY name
ORDER BY name
