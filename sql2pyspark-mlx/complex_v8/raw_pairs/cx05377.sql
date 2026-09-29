WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE student_id <= 6
      AND grade_status = 'INCOMPLETE'
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at >= TIMESTAMP '2025-01-07 10:00:00'
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (ORDER BY enrolled_at NULLS LAST, id) AS enrolled_at_rank,
        grade_status_len - LAG(grade_status_len) OVER (ORDER BY enrolled_at NULLS LAST, id) AS grade_status_len_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at_quarter DESC, id), 3) AS enrolled_at_quarter_percent_rank,
        SUM(score) OVER (ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score
    FROM filtered
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    enrolled_at_quarter,
    enrolled_at_rank,
    grade_status_len_change,
    enrolled_at_quarter_percent_rank,
    running_score,
    COALESCE(enrolled_at_rank, 0) AS enrolled_at_rank_filled
FROM windowed
ORDER BY score NULLS LAST, grade_status_len_change NULLS LAST
