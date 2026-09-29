WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ROUND(score * 1.1, 2) AS score_adj,
        enrolled_at + INTERVAL 15 MINUTES AS enrolled_at_plus_15m
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-25 14:00:00'
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM recent_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status <> 'FAIL'
        )
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        ROUND(PERCENT_RANK() OVER (ORDER BY score ASC), 3) AS score_percent_rank,
        ROUND((UNIX_TIMESTAMP(enrolled_at_plus_15m) - UNIX_TIMESTAMP(LAG(enrolled_at_plus_15m) OVER (ORDER BY score ASC, id))) / 60, 1) AS minutes_since_prev_enrolled_at_plus_15m
    FROM filtered
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at_plus_15m,
    rows_in_grade_status,
    score_percent_rank,
    GREATEST(rows_in_grade_status, score) AS greatest_rows_in_grade_status_score
FROM sequenced
ORDER BY course_id, rows_in_grade_status DESC
