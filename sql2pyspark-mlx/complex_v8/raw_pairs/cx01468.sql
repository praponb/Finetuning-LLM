WITH scoped AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE credits <= 3
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        dept,
        level,
        COALESCE(credits, 0) AS credits_filled,
        ABS(credits - 2) AS credits_distance,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
    FROM scoped
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
        )
)
SELECT
    id,
    dept,
    credits_filled,
    credits_distance,
    dept_slug
FROM without_matches
ORDER BY dept NULLS LAST, dept_slug
