WITH cleaned_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE level IN ('core', 'advanced')
),
stacked AS (
    -- Stack the branches
    SELECT id AS ref_id, credits
    FROM cleaned_courses
    UNION
    SELECT id AS ref_id, course_id AS credits
    FROM enrollments
)
SELECT ref_id, COUNT(*) AS n_rows, SUM(credits) AS total_credits
FROM stacked
GROUP BY ref_id
ORDER BY ref_id
