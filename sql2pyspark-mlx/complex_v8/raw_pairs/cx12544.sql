WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at + INTERVAL 2 HOURS AS enrolled_at_plus_2h,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        ABS(score - 62.5) AS score_distance
    FROM enrollments
    WHERE (score > 32.7 OR student_id <= 5)
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(term_or_unknown, 'ALL') AS term_or_unknown,
    course_id,
    MAX(enrolled_at) AS last_enrolled_at
FROM qualified
GROUP BY GROUPING SETS ((term_or_unknown, course_id), (term_or_unknown))
