WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE HOUR(played_at) BETWEEN 8 AND 12
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM valid_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        device,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY device, session_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY events ORDER BY session_end, device, session_id) AS seq_no,
        ROUND(session_id - AVG(session_id) OVER (PARTITION BY session_id), 2) AS session_id_vs_session_id_avg,
        SUM(events) OVER (PARTITION BY session_id) AS session_id_sum_events,
        RANK() OVER (PARTITION BY events, device ORDER BY session_start DESC, device, session_id) AS session_start_rank,
        DENSE_RANK() OVER (PARTITION BY events ORDER BY session_end, device, session_id) AS session_end_dense_rank,
        DENSE_RANK() OVER (PARTITION BY events, device ORDER BY session_start DESC, device, session_id) AS session_start_dense_rank
    FROM session_summary
),
leaders AS (
    -- First row per group
    SELECT
        device,
        session_id,
        session_start,
        session_end,
        events,
        duration_min,
        seq_no,
        session_id_vs_session_id_avg,
        session_id_sum_events,
        session_start_rank,
        session_end_dense_rank
    FROM windowed
    WHERE session_start_dense_rank = 1
      AND events <= 5
)
-- Final projection
SELECT
    device,
    session_start,
    session_end,
    events,
    duration_min,
    seq_no,
    session_id_vs_session_id_avg,
    session_id_sum_events,
    session_start_rank,
    session_end_dense_rank,
    DATEDIFF(DATE '2026-01-01', session_end) AS days_since_session_end,
    ROUND(session_end_dense_rank / NULLIF(seq_no, 0), 3) AS session_end_dense_rank_per_seq_no
FROM leaders
ORDER BY session_end_dense_rank DESC, session_end DESC
