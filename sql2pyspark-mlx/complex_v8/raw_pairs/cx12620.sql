WITH unioned AS (
    -- Stack the branches
    SELECT course_code, level
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'A%'
    UNION ALL
    SELECT dept AS course_code, level
    FROM courses
    WHERE credits >= 2
)
SELECT course_code, COUNT(*) AS n_rows
FROM unioned
GROUP BY course_code
ORDER BY course_code
