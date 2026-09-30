WITH scoped AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE cohort IS NOT NULL
),
stacked AS (
    -- Stack the branches
    SELECT 'student' AS src, student_name AS category, enrolled_on
    FROM scoped
    WHERE LOWER(student_name) LIKE '%na%'
    UNION ALL
    SELECT 'student_2' AS src, major AS category, enrolled_on
    FROM students
    WHERE cohort IN ('C2024', 'C2022')
)
SELECT src, COUNT(*) AS n_rows, MAX(enrolled_on) AS latest_enrolled_on
FROM stacked
GROUP BY src
ORDER BY src
