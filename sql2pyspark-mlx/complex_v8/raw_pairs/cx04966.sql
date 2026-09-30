WITH base_enrollments AS (
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
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
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
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, burst_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        grade_status,
        burst_id,
        session_end,
        n_events,
        session_score,
        duration_min,
        FIRST_VALUE(burst_id) IGNORE NULLS OVER (ORDER BY duration_min ASC NULLS LAST, grade_status, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_burst_id,
        NTH_VALUE(n_events, 2) OVER (ORDER BY duration_min ASC NULLS LAST, grade_status, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_events,
        NTH_VALUE(session_score, 2) OVER (ORDER BY duration_min ASC NULLS LAST, grade_status, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_score,
        duration_min - LAG(duration_min) OVER (ORDER BY duration_min ASC NULLS LAST, grade_status, burst_id) AS duration_min_change
    FROM sessions
)
-- Final projection
SELECT
    grade_status,
    burst_id,
    n_events,
    session_score,
    duration_min,
    first_known_burst_id,
    second_session_score,
    duration_min_change,
    ROUND(duration_min_change * 1.1, 2) AS duration_min_change_adj,
    DATEDIFF(DATE '2026-01-01', session_end) AS days_since_session_end
FROM metrics
WHERE session_score >= 120.0
