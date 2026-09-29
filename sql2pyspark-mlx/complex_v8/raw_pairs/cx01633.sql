WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        term,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, burst_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(session_start, 3) OVER (PARTITION BY events ORDER BY session_end, term, burst_id) AS next3_session_start,
        ROUND(AVG(duration_min) OVER (PARTITION BY burst_id, term), 2) AS burst_id_term_avg_duration_min,
        COUNT(*) OVER (PARTITION BY burst_id, term) AS rows_in_burst_id_term,
        ROUND(burst_id / NULLIF(SUM(burst_id) OVER w_burst_id_term, 0), 4) AS burst_id_share_of_burst_id_term,
        DENSE_RANK() OVER w_events_by_session_end_ties AS session_end_dense_rank,
        LAG(events) OVER w_events_by_session_end_2 AS prev_events
    FROM session_summary
    WINDOW w_burst_id_term AS (PARTITION BY burst_id, term), w_events_by_session_end_ties AS (PARTITION BY events ORDER BY session_end DESC NULLS FIRST), w_events_by_session_end_2 AS (PARTITION BY events ORDER BY session_end DESC NULLS FIRST, term, burst_id)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(duration_min, 0.0) AS duration_min_filled,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM metrics
)
SELECT
    term,
    session_start,
    session_end,
    events,
    duration_min,
    next3_session_start,
    burst_id_term_avg_duration_min,
    rows_in_burst_id_term,
    burst_id_share_of_burst_id_term,
    session_end_dense_rank,
    prev_events,
    duration_min_filled,
    ROUND(session_end_dense_rank / NULLIF(rows_in_burst_id_term, 0), 3) AS session_end_dense_rank_per_rows_in_burst_id_term
FROM scored
