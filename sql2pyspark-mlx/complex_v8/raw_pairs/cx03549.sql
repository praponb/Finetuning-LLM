WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM cleaned_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        student_id,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, session_id
),
labeled AS (
    -- Label and score rows
    SELECT *, n_events + duration_min AS n_events_plus_duration_min
    FROM per_session
    WHERE n_events BETWEEN 2 AND 5
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', session_start) AS days_since_session_start
    FROM labeled
)
-- Subtotals with CUBE
SELECT
    student_id,
    n_events,
    MAX(session_start) AS last_session_start,
    SUM(session_id) AS total_session_id
FROM annotated
GROUP BY CUBE(student_id, n_events)
ORDER BY student_id NULLS LAST, n_events NULLS LAST
