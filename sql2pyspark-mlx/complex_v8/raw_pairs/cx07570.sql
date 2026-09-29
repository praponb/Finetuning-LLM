WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled
    FROM courses
    WHERE credits > 2
      AND level IS NOT NULL
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.enrolled_at >= TIMESTAMP '2025-01-25 11:00:00'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, credits_filled, credits
    FROM with_activity
) src
UNPIVOT (
    value FOR field IN (credits_filled, credits)
) unp
