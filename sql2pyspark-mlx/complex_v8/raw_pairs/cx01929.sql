WITH scoped AS (
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
    WHERE grade_status = 'WITHDRAWN'
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        course_id,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY course_id, session_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        course_id,
        session_id,
        event_count,
        duration_min,
        RANK() OVER (PARTITION BY course_id ORDER BY session_end, course_id, session_id) AS session_end_rank,
        COUNT(*) OVER (PARTITION BY YEAR(session_end)) AS rows_in_session_end_year,
        ROUND(duration_min - AVG(duration_min) OVER (PARTITION BY YEAR(session_end)), 2) AS duration_min_vs_session_end_year_avg,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY session_end, course_id, session_id) AS session_end_dense_rank,
        session_id - LAG(session_id) OVER (PARTITION BY course_id ORDER BY session_end, course_id, session_id) AS session_id_change,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id ORDER BY session_end, course_id, session_id), 3) AS session_end_cume_dist
    FROM sessions
)
SELECT
    course_id,
    session_id,
    event_count,
    session_end_rank,
    rows_in_session_end_year,
    duration_min_vs_session_end_year_avg,
    session_end_dense_rank,
    session_id_change,
    session_end_cume_dist,
    COALESCE(event_count, 0) AS event_count_filled
FROM metrics
WHERE session_id_change < 4
ORDER BY rows_in_session_end_year DESC, duration_min_vs_session_end_year_avg, course_id, session_id
LIMIT 5
