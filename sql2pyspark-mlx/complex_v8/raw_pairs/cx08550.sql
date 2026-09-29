WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE course_id <= 6
),
with_prev AS (
    -- Previous enrolled_at per term_or_unknown
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term_or_unknown ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY term_or_unknown ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        term_or_unknown,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term_or_unknown, burst_id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(term_or_unknown)) AS term_or_unknown_clean,
        ROUND(burst_id / NULLIF(duration_min, 0), 3) AS burst_id_per_duration_min
    FROM sessions
)
-- Final projection
SELECT
    term_or_unknown,
    burst_id,
    session_start,
    session_end,
    events,
    duration_min,
    term_or_unknown_clean,
    burst_id_per_duration_min,
    YEAR(session_start) AS session_start_year,
    TO_DATE(session_start) AS session_start_date
FROM labeled
WHERE term_or_unknown_clean IS NOT NULL
