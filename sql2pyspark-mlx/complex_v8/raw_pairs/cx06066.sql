WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 73.7 THEN 'A' WHEN score >= 38.3 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per score_tier
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY score_tier ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY score_tier ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        score_tier,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY score_tier, burst_id
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM per_session
    WHERE burst_id < (SELECT MAX(burst_id) FROM per_session)
)
-- Final projection
SELECT
    score_tier,
    burst_id,
    session_end,
    events,
    duration_min
FROM with_activity
ORDER BY duration_min DESC, events DESC
