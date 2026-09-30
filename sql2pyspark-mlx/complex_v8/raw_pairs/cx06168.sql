WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        DATE_ADD(enrolled_on, 7) AS enrolled_on_plus_7d
    FROM students
    WHERE enrolled_on < DATE '2025-03-15'
),
unioned AS (
    -- Stack the branches
    SELECT cohort, enrolled_on
    FROM valid
    WHERE gpa > 2.5
    UNION ALL
    SELECT major, enrolled_on
    FROM students
    UNION ALL
    SELECT cohort, enrolled_on
    FROM students
    WHERE enrolled_on < DATE '2025-07-18'
)
SELECT cohort, COUNT(*) AS n_rows, MAX(enrolled_on) AS latest_enrolled_on
FROM unioned
GROUP BY cohort
ORDER BY cohort
