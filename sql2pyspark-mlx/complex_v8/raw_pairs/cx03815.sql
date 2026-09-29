WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per term_or_unknown
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term_or_unknown ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY term_or_unknown ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        term_or_unknown,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term_or_unknown, session_id
)
SELECT term_or_unknown, session_id, field, reading
FROM (
    SELECT term_or_unknown, session_id, duration_min, session_score
    FROM per_session
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (duration_min AS duration_min_v, session_score AS m_session_score)
) unp
