WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        grade_status,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, session_id
),
grouped AS (
    -- Summarise per group
    SELECT
        session_id,
        DATE_FORMAT(session_end, 'yyyy-MM') AS session_end_ym,
        ROUND(SUM(session_id) / NULLIF(SUM(duration_min), 0), 3) AS session_id_to_duration_min_ratio,
        SUM(session_id) AS total_session_id,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        MAX(session_start) AS last_session_start
    FROM session_summary
    GROUP BY session_id, DATE_FORMAT(session_end, 'yyyy-MM')
)
SELECT
    session_id,
    session_end_ym,
    session_id_to_duration_min_ratio,
    last_session_start
FROM grouped
WHERE session_id >= 2
ORDER BY session_id_to_duration_min_ratio DESC
