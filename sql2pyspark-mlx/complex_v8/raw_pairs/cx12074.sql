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
    WHERE term NOT IN ('2025T2', '2024T2')
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        grade_status,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, session_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE n_events > (SELECT AVG(n_events) FROM per_session)
),
tagged AS (
    -- Label and score rows
    SELECT
        grade_status,
        session_id,
        session_end,
        n_events,
        duration_min,
        GREATEST(session_id, duration_min) AS greatest_session_id_duration_min,
        LEAST(duration_min, n_events) AS least_duration_min_n_events,
        COALESCE(duration_min, 0.0) AS duration_min_filled
    FROM qualified
    WHERE grade_status = 'INCOMPLETE'
)
-- Final projection
SELECT
    grade_status,
    session_id,
    session_end,
    n_events,
    duration_min,
    least_duration_min_n_events,
    duration_min_filled,
    CASE WHEN n_events >= 5 THEN 'A' WHEN n_events >= 3 THEN 'B' ELSE 'C' END AS n_events_tier,
    ROUND(duration_min / NULLIF(duration_min_filled, 0), 3) AS duration_min_per_duration_min_filled
FROM tagged
ORDER BY n_events_tier DESC, duration_min_filled
