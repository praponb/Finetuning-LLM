WITH prepared AS (
    -- Keep valid rows and derive helper columns
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
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        term,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, session_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER w_session_id_term_by_session_s AS session_start_quintile,
        NTILE(10) OVER (PARTITION BY session_id, term ORDER BY session_start, term, session_id) AS session_start_decile,
        SUM(session_id) OVER (PARTITION BY session_id, term ORDER BY session_start, term, session_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS session_id_sum_3,
        ROW_NUMBER() OVER w_session_id_term_by_session_s AS session_start_seq,
        ROUND(PERCENT_RANK() OVER w_session_id_term_by_session_s, 3) AS session_start_percent_rank
    FROM session_summary
    WINDOW w_session_id_term_by_session_s AS (PARTITION BY session_id, term ORDER BY session_start, term, session_id)
)
-- Subtotals with GROUPING SETS
SELECT
    session_start_quintile,
    session_start_seq,
    ROUND(100 * COUNT(CASE WHEN term = '2025T1' THEN 1 END) / COUNT(*), 1) AS pct_term_2025t1,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT term) AS distinct_terms
FROM with_trends
GROUP BY GROUPING SETS ((session_start_quintile, session_start_seq), (session_start_quintile), (session_start_seq))
ORDER BY session_start_quintile NULLS LAST, session_start_seq NULLS LAST
