WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(prev_enrolled_at) AS prev_enrolled_at_year,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM sessionized
    WHERE session_id BETWEEN 2 AND 4
)
-- Subtotals with GROUPING SETS
SELECT
    term,
    session_id,
    student_id,
    COUNT(DISTINCT student_id) AS distinct_student_ids,
    ROUND(STDDEV(new_session_flag), 2) AS stddev_new_session_flag,
    COUNT(*) FILTER (WHERE grade_status = 'INCOMPLETE') AS n_grade_status_incomplete
FROM labeled
GROUP BY GROUPING SETS ((term, session_id, student_id), (term), (session_id), ())
ORDER BY term NULLS LAST, session_id NULLS LAST, student_id NULLS LAST
