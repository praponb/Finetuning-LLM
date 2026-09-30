WITH scoped AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
    WHERE term IN ('2025T1', '2024T1', '2024T2')
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        grade_status,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, burst_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(burst_id, 0) AS burst_id_filled,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM sessions
    WHERE HOUR(session_start) BETWEEN 10 AND 15
)
-- Final projection
SELECT
    grade_status,
    burst_id,
    session_end,
    event_count,
    duration_min,
    burst_id_filled,
    grade_status_prefix,
    DATEDIFF(DATE '2025-12-31', session_start) AS days_since_session_start
FROM derived
ORDER BY grade_status_prefix
