WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id >= 3
        )
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id ORDER BY enrolled_at DESC), 3) AS enrolled_at_percent_rank,
        NTILE(10) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS enrolled_at_decile,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at
    FROM qualified
)
SELECT *
FROM (
    SELECT student_id, first_enrolled_at, grade_status, enrolled_at_decile
    FROM ranked
) src
PIVOT (
    AVG(enrolled_at_decile) AS avg_v, COUNT(enrolled_at_decile) AS n
    FOR grade_status IN ('PASS', 'WITHDRAWN', 'INCOMPLETE')
)
