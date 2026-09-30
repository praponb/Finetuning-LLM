WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    prev_enrolled_at,
    new_session_flag,
    session_id,
    CASE WHEN session_id >= 4 THEN 'HIGH' WHEN session_id >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS session_id_tier
FROM sessionized
