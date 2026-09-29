WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.1, 2) AS score_adj
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        grade_status,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(score_adj) AS session_score_adj,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, session_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(grade_status, '#', CAST(session_id AS STRING)) AS grade_status_ref
    FROM session_summary
    WHERE MONTH(session_start) IN (1, 2)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(session_score_adj / NULLIF(n_events, 0), 3) AS session_score_adj_per_n_events
    FROM enriched
    WHERE HOUR(session_end) BETWEEN 10 AND 15
)
-- Final projection
SELECT
    grade_status,
    session_start,
    session_end,
    n_events,
    session_score_adj,
    duration_min,
    grade_status_ref,
    ROUND(session_score_adj * 1.1, 2) AS session_score_adj_adj
FROM derived
