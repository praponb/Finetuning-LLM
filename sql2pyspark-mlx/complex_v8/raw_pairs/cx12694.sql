WITH unioned AS (
    -- Stack the branches
    SELECT 'course' AS source, course_code, credits
    FROM courses
    WHERE credits > 2
      AND dept = 'Bio'
    UNION ALL
    SELECT 'course_2' AS source, dept, id
    FROM courses
    WHERE credits >= 2
      AND dept = 'Biz'
)
SELECT source, COUNT(*) AS n_rows, SUM(credits) AS total_credits
FROM unioned
GROUP BY source
ORDER BY source
