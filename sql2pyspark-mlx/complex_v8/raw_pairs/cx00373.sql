WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        course_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY course_id, burst_id
),
per_group AS (
    -- Summarise per group
    SELECT
        course_id,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        ROUND(SUM(burst_id) / NULLIF(SUM(n_events), 0), 3) AS burst_id_to_n_events_ratio,
        PERCENTILE_APPROX(duration_min, 0.5) AS median_duration_min,
        MAX(session_start) AS last_session_start,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        PERCENTILE_APPROX(n_events, 0.5) AS median_n_events
    FROM per_session
    GROUP BY course_id, DATE_TRUNC('MONTH', session_start)
    HAVING SUM(n_events) > 3
)
-- Final projection
SELECT
    course_id,
    session_start_month,
    burst_id_to_n_events_ratio,
    median_duration_min,
    last_session_start,
    distinct_course_ids,
    median_n_events,
    DATEDIFF(DATE '2025-12-31', last_session_start) AS days_since_last_session_start
FROM per_group
ORDER BY median_n_events DESC NULLS FIRST
