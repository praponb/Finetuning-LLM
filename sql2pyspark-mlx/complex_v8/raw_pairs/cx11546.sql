WITH stacked AS (
    -- Stack the branches
    SELECT 'course' AS src, level, credits
    FROM courses
    WHERE course_code IS NOT NULL
    UNION ALL
    SELECT 'course_2' AS src, dept, credits
    FROM courses
    WHERE dept IN ('CS', 'Bio', 'Art')
)
SELECT src, COUNT(*) AS n_rows, SUM(credits) AS total_credits
FROM stacked
GROUP BY src
ORDER BY src
