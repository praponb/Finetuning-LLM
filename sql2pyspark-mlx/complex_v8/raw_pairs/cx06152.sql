WITH base_plays AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM base_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessionized
    WHERE burst_id > (SELECT AVG(burst_id) FROM sessionized)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_all_by_burst_id_ties AS burst_id_rank,
        ROW_NUMBER() OVER (ORDER BY burst_id DESC NULLS FIRST, id) AS burst_id_seq,
        ROUND(burst_id - AVG(burst_id) OVER w_is_new_session, 2) AS burst_id_vs_is_new_session_avg,
        DENSE_RANK() OVER w_is_new_session_device_by_pre AS prev_played_at_dense_rank
    FROM screened
    WINDOW w_all_by_burst_id_ties AS (ORDER BY burst_id DESC NULLS FIRST), w_is_new_session AS (PARTITION BY is_new_session), w_is_new_session_device_by_pre AS (PARTITION BY is_new_session, device ORDER BY prev_played_at, id)
)
-- Final projection
SELECT
    id,
    track_id,
    device,
    source,
    burst_id,
    burst_id_rank,
    burst_id_seq,
    burst_id_vs_is_new_session_avg,
    DATE_FORMAT(prev_played_at, 'MMM yyyy') AS prev_played_at_label
FROM with_windows
WHERE device = 'phone'
ORDER BY prev_played_at_label, id
LIMIT 20
