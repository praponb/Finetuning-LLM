WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE NOT is_skipped
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(played_at) OVER (PARTITION BY device ORDER BY starts_session ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY days_since_played_at DESC) AS days_since_played_at_dense_rank,
        MIN(starts_session) OVER (PARTITION BY device ORDER BY starts_session ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_starts_session_so_far,
        DENSE_RANK() OVER w_device_by_starts_session AS starts_session_dense_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id, starts_session), 0), 4) AS ms_played_share_of_track_id_starts_session,
        DENSE_RANK() OVER (ORDER BY starts_session) AS starts_session_dense_rank_2
    FROM sessionized
    WINDOW w_device_by_starts_session AS (PARTITION BY device ORDER BY starts_session ASC, id)
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    is_skipped,
    source,
    prev_played_at,
    starts_session,
    session_no,
    last_played_at,
    starts_session_dense_rank,
    ms_played_share_of_track_id_starts_session,
    starts_session_dense_rank_2,
    SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
    CASE WHEN days_since_played_at >= 595 THEN 'A' WHEN days_since_played_at >= 210 THEN 'B' ELSE 'C' END AS days_since_played_at_tier
FROM sequenced
WHERE days_since_played_at_dense_rank <= 9
ORDER BY track_id DESC
