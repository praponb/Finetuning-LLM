WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        term,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, session_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        n_events,
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        ROUND(100 * COUNT(CASE WHEN term IN ('2024T1', '2025T2', '2024T2') THEN 1 END) / COUNT(*), 1) AS pct_term_in_2024t1_2025t2_2024t2,
        MAX(session_start) AS last_session_start,
        ROUND(SUM(duration_min) / NULLIF(SUM(session_score), 0), 3) AS duration_min_to_session_score_ratio,
        COUNT(*) AS row_count,
        SUM(CASE WHEN term <> '2025T1' THEN duration_min ELSE 0 END) AS duration_min_term_2025t1,
        ROUND(AVG(duration_min), 2) AS avg_duration_min
    FROM per_session
    WHERE session_end < TIMESTAMP '2025-02-05 09:00:00'
    GROUP BY DATE_TRUNC('MONTH', session_start), n_events, DATE_TRUNC('MONTH', session_end)
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    session_start_month,
    session_end_month,
    pct_term_in_2024t1_2025t2_2024t2,
    last_session_start,
    duration_min_to_session_score_ratio,
    avg_duration_min,
    GREATEST(n_events, row_count) AS greatest_n_events_row_count,
    DAYOFWEEK(session_end_month) AS session_end_month_dayofweek
FROM rollup_base
ORDER BY session_start_month DESC, session_end_month DESC NULLS FIRST
