WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        term,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, session_no
),
summary AS (
    -- Group-level totals
    SELECT
        session_no,
        n_events,
        ROUND(AVG(duration_min), 2) AS avg_duration_min,
        PERCENTILE_APPROX(n_events, 0.5) AS median_n_events,
        SUM(n_events) FILTER (WHERE term IS NOT NULL) AS n_events_term_is_not_null,
        MIN(n_events) AS min_n_events,
        ROUND(SUM(session_no) / NULLIF(SUM(n_events), 0), 3) AS session_no_to_n_events_ratio
    FROM sessions
    GROUP BY session_no, n_events
)
SELECT
    session_no,
    n_events_term_is_not_null,
    min_n_events,
    session_no_to_n_events_ratio
FROM summary
ORDER BY n_events_term_is_not_null, min_n_events
