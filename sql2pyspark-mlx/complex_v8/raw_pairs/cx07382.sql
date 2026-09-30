WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        CASE WHEN gpa >= 3.3 THEN 'large' WHEN gpa >= 2.5 THEN 'medium' ELSE 'small' END AS gpa_tier
    FROM students
    WHERE student_name IS NOT NULL
),
unioned AS (
    -- Stack the branches
    SELECT
        id AS qty,
        enrolled_on AS day,
        gpa AS amount,
        'student' AS src
    FROM filtered
    UNION ALL
    SELECT
        id AS qty,
        enrolled_on AS day,
        gpa AS amount,
        'student_2' AS src
    FROM students
    UNION ALL
    SELECT
        id AS qty,
        enrolled_on AS day,
        gpa AS amount,
        'student_3' AS src
    FROM students
    WHERE major IS NOT NULL
)
SELECT src, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(day) AS latest_day
FROM unioned
GROUP BY src
ORDER BY src
