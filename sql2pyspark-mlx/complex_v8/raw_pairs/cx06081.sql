WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term = '2025T1'
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        course_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY course_id, burst_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        course_id,
        burst_id,
        session_start,
        event_count,
        ROW_NUMBER() OVER (PARTITION BY event_count ORDER BY session_start, course_id, burst_id) AS seq_no,
        LAST_VALUE(duration_min) OVER (PARTITION BY event_count ORDER BY session_start, course_id, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_min,
        LEAD(session_end, 2) OVER (PARTITION BY burst_id, event_count ORDER BY session_end, course_id, burst_id) AS next2_session_end,
        LEAD(session_end) OVER (PARTITION BY burst_id, event_count ORDER BY session_end, course_id, burst_id) AS next_session_end,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', session_end)) AS rows_in_session_end_month
    FROM session_summary
)
SELECT
    course_id,
    burst_id,
    session_start,
    seq_no,
    last_duration_min,
    next2_session_end,
    next_session_end,
    rows_in_session_end_month,
    ROUND(last_duration_min * 1.1, 2) AS last_duration_min_adj,
    GREATEST(seq_no, event_count) AS greatest_seq_no_event_count
FROM with_windows
ORDER BY next_session_end DESC
