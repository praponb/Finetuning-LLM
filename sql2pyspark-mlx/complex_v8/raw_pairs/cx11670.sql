WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled
    FROM courses
    WHERE level IN ('core', 'advanced')
)
SELECT credits_filled, credits
FROM cleaned
UNION ALL
SELECT id AS credits_filled, student_id AS credits
FROM enrollments
ORDER BY credits_filled
