WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        course_id,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(score_filled) AS session_score_filled,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY course_id, session_id
),
grouped AS (
    -- Summarise per group
    SELECT
        session_id,
        YEAR(session_end) AS session_end_year,
        CASE WHEN duration_min >= 168.0 THEN 'large' WHEN duration_min >= 84.0 THEN 'medium' ELSE 'small' END AS duration_min_tier,
        MIN(session_id) AS min_session_id,
        MAX(session_start) AS last_session_start
    FROM per_session
    GROUP BY session_id, YEAR(session_end), CASE WHEN duration_min >= 168.0 THEN 'large' WHEN duration_min >= 84.0 THEN 'medium' ELSE 'small' END
    HAVING SUM(n_events) > 3
)
-- Final projection
SELECT
    session_id,
    session_end_year,
    duration_min_tier,
    min_session_id,
    SUBSTRING(TRIM(duration_min_tier), 1, 4) AS duration_min_tier_prefix
FROM grouped
