WITH filtered AS (
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
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        term,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, session_id
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        term,
        session_id,
        MAX(session_start) AS last_session_start,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM per_session
    GROUP BY 1, 2, 3
    HAVING MAX(duration_min) > 48.0
)
-- Subtotals with ROLLUP
SELECT
    term,
    session_id,
    COUNT(DISTINCT term) AS distinct_terms,
    ROUND(STDDEV(session_id), 2) AS stddev_session_id
FROM totals
GROUP BY ROLLUP(term, session_id)
ORDER BY term NULLS LAST, session_id NULLS LAST
