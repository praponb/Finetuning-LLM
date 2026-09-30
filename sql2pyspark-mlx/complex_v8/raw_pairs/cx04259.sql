WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 8 AND 14
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
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
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY events NULLS LAST) AS events_dense_rank,
        LAG(events, 1, 0) OVER w_all_by_events AS prev_events,
        ROUND(AVG(burst_id) OVER (PARTITION BY term, burst_id), 2) AS term_burst_id_avg_burst_id,
        DENSE_RANK() OVER w_all_by_events AS events_dense_rank_2,
        MAX(duration_min) OVER w_term_burst_id AS term_burst_id_max_duration_min
    FROM per_session
    WINDOW w_all_by_events AS (ORDER BY events NULLS LAST, term, burst_id), w_term_burst_id AS (PARTITION BY term, burst_id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        term,
        burst_id,
        session_start,
        session_end,
        term_burst_id_avg_burst_id,
        events_dense_rank_2,
        term_burst_id_max_duration_min,
        MAX(burst_id) OVER w_burst_id AS burst_id_max_burst_id,
        ROUND(AVG(events_dense_rank) OVER (PARTITION BY burst_id), 2) AS burst_id_avg_events_dense_rank,
        ROUND(events_dense_rank_2 / NULLIF(SUM(events_dense_rank_2) OVER w_burst_id, 0), 4) AS events_dense_rank_2_share_of_burst_id
    FROM with_trends
    WINDOW w_burst_id AS (PARTITION BY burst_id)
)
SELECT DISTINCT burst_id, term, session_end
FROM sequenced
ORDER BY burst_id DESC, session_end DESC
