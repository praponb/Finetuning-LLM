WITH recent_enrollments AS (
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
    WHERE HOUR(enrolled_at) BETWEEN 9 AND 13
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        term,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, session_id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM sessions
    WHERE n_events < (SELECT MAX(n_events) FROM sessions)
),
scored AS (
    -- Label and score rows
    SELECT
        term,
        session_id,
        session_start,
        session_end,
        duration_min,
        ROUND(n_events / NULLIF(session_id, 0), 3) AS n_events_per_session_id,
        CASE WHEN session_id >= 4 THEN 'HIGH' WHEN session_id >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS session_id_tier,
        ABS(n_events - 4) AS n_events_distance
    FROM qualified
)
SELECT
    COALESCE(term, 'ALL') AS term,
    COALESCE(session_id_tier, 'ALL') AS session_id_tier,
    ROUND(AVG(n_events_distance), 2) AS avg_n_events_distance,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list
FROM scored
GROUP BY CUBE(term, session_id_tier)
ORDER BY term NULLS LAST, session_id_tier NULLS LAST
