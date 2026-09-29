WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE gpa > 2.7
      AND is_international = FALSE
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.grade_status IN ('WITHDRAWN', 'FAIL', 'PASS')
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(enrolled_on, 2) OVER (ORDER BY enrolled_on DESC, id) AS prev2_enrolled_on,
        ROUND(gpa / NULLIF(SUM(gpa) OVER (PARTITION BY is_international), 0), 4) AS gpa_share_of_is_international,
        NTH_VALUE(enrolled_on, 2) OVER (ORDER BY enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_on
    FROM qualified
)
SELECT *
FROM (
    SELECT second_enrolled_on, prev2_enrolled_on, major, gpa_share_of_is_international
    FROM metrics
) src
PIVOT (
    MAX(gpa_share_of_is_international)
    FOR major IN ('CS', 'Math', 'Biz', 'Bio')
)
ORDER BY second_enrolled_on, prev2_enrolled_on
