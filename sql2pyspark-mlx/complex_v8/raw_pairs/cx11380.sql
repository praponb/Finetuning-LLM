WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 77.0 THEN 'large' WHEN score >= 26.2 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        student_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, burst_id
),
stats AS (
    -- Summarise per group
    SELECT
        burst_id,
        student_id,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        PERCENTILE_APPROX(n_events, 0.5) AS median_n_events,
        SUM(burst_id) AS total_burst_id,
        MIN(session_end) AS first_session_end
    FROM sessions
    GROUP BY 1, 2
    HAVING SUM(duration_min) > 72.0
)
-- Final projection
SELECT
    burst_id,
    student_id,
    distinct_student_ids,
    median_n_events,
    total_burst_id,
    first_session_end
FROM stats
ORDER BY total_burst_id DESC, burst_id NULLS LAST
