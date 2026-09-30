WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
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
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY grade_status, MONTH(session_end) ORDER BY event_count, grade_status, burst_id) AS event_count_decile,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_count DESC NULLS FIRST, grade_status, burst_id), 3) AS event_count_percent_rank,
        ROUND(CUME_DIST() OVER w_all_by_event_count, 3) AS event_count_cume_dist,
        LAG(event_count) OVER (ORDER BY event_count DESC NULLS FIRST, grade_status, burst_id) AS prev_event_count,
        LAST_VALUE(session_start) IGNORE NULLS OVER (PARTITION BY grade_status, MONTH(session_end) ORDER BY event_count, grade_status, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_start
    FROM per_session
    WINDOW w_all_by_event_count AS (ORDER BY event_count DESC NULLS FIRST, grade_status, burst_id)
)
SELECT
    grade_status,
    burst_id,
    event_count,
    duration_min,
    event_count_decile,
    event_count_percent_rank,
    event_count_cume_dist,
    prev_event_count,
    last_known_session_start,
    SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix,
    LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
FROM metrics
ORDER BY event_count_percent_rank DESC
