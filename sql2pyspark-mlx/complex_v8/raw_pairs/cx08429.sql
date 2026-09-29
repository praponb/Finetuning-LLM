WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM courses
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM cleaned
    WHERE credits < (SELECT MAX(credits) FROM cleaned)
)
-- Subtotals with CUBE
SELECT
    dept,
    level,
    COUNT(DISTINCT id) AS distinct_ids
FROM screened
GROUP BY CUBE(dept, level)
ORDER BY dept NULLS LAST, level NULLS LAST
