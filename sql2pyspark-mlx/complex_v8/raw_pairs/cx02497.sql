WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
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
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, burst_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        grade_status,
        burst_id,
        session_start,
        session_end,
        n_events,
        duration_min,
        DATE_TRUNC('HOUR', session_end) AS session_end_hour,
        duration_min - n_events AS duration_min_minus_n_events
    FROM sessions
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(duration_min - AVG(duration_min) OVER (PARTITION BY burst_id), 2) AS duration_min_vs_burst_id_avg,
        LEAD(duration_min, 2) OVER (PARTITION BY MONTH(session_end) ORDER BY duration_min, grade_status, burst_id) AS next2_duration_min,
        ROUND((UNIX_TIMESTAMP(session_end_hour) - UNIX_TIMESTAMP(LAG(session_end_hour) OVER w_all_by_duration_min_minus_n)) / 60, 1) AS minutes_since_prev_session_end_hour,
        MAX(burst_id) OVER (PARTITION BY burst_id) AS burst_id_max_burst_id
    FROM enriched
    WINDOW w_all_by_duration_min_minus_n AS (ORDER BY duration_min_minus_n_events, grade_status, burst_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT grade_status, burst_id, measure, amount
FROM (
    SELECT grade_status, burst_id, duration_min, minutes_since_prev_session_end_hour, next2_duration_min, duration_min_minus_n_events
    FROM sequenced
) src
UNPIVOT (
    amount FOR measure IN (duration_min, minutes_since_prev_session_end_hour, next2_duration_min, duration_min_minus_n_events)
) unp
ORDER BY grade_status, burst_id, measure
