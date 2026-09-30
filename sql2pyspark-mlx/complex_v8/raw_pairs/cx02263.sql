WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(score) OVER (PARTITION BY term, course_id ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), term) AS rows_in_enrolled_at_month_term
    FROM valid_enrollments
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status IN ('WITHDRAWN', 'PASS')
        )
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id IS NOT NULL
              AND e.score BETWEEN 26.2 AND 73.0
        )
)
SELECT
    id,
    student_id,
    course_id,
    term
FROM with_activity
WHERE student_id >= 4
ORDER BY student_id NULLS LAST, course_id DESC, id
LIMIT 10
