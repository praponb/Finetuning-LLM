WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE (course_id <= 2 OR score < 57.3)
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score > 11.1
              AND e.term IS NOT NULL
        )
),
grouped AS (
    -- Group-level totals
    SELECT
        grade_status,
        term,
        ROUND(100 * COUNT(CASE WHEN grade_status = 'FAIL' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_fail,
        COUNT(*) FILTER (WHERE grade_status IS NOT NULL) AS n_grade_status_is_not_null,
        COUNT(*) AS row_count
    FROM with_activity
    GROUP BY grade_status, term
)
SELECT
    grade_status,
    n_grade_status_is_not_null,
    row_count,
    ROUND(row_count / NULLIF(pct_grade_status_fail, 0), 3) AS row_count_per_pct_grade_status_fail
FROM grouped
WHERE term = '2024T2'
ORDER BY row_count_per_pct_grade_status_fail DESC
