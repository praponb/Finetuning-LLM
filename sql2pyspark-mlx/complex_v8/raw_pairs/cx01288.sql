WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 80.2 THEN 'A' WHEN score >= 20.6 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE term = '2025T2'
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        grade_status,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, session_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessions
    WHERE duration_min > (SELECT AVG(duration_min) FROM sessions)
)
-- Final projection
SELECT
    grade_status,
    session_end,
    event_count,
    session_score,
    duration_min
FROM without_matches
WHERE grade_status <> 'PASS'
ORDER BY duration_min
