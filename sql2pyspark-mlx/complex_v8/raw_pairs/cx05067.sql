WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        CONCAT_WS('-', source, device) AS source_device_label
    FROM plays
    WHERE is_skipped = FALSE
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY is_skipped_flag, listener_id ORDER BY is_skipped_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAG(played_at) OVER w_track_id_listener_id_by_play AS prev_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device, DATE_TRUNC('MONTH', played_at) ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_2,
        MAX(is_skipped_flag) OVER (PARTITION BY is_skipped_flag, listener_id ORDER BY is_skipped_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_is_skipped_flag_so_far,
        ROUND(is_skipped_flag / NULLIF(SUM(is_skipped_flag) OVER w_track_id_is_skipped, 0), 4) AS is_skipped_flag_share_of_track_id_is_skipped,
        is_skipped_flag - LAG(is_skipped_flag) OVER (PARTITION BY is_skipped, track_id ORDER BY played_at, id) AS is_skipped_flag_change,
        RANK() OVER w_track_id_listener_id_by_play_2 AS played_at_rank,
        NTILE(4) OVER w_is_skipped_flag_listener_id_3 AS is_skipped_flag_quartile,
        LEAD(is_skipped_flag) OVER w_device_played_at_month_by_pl_2 AS next_is_skipped_flag,
        ROW_NUMBER() OVER (PARTITION BY is_skipped, track_id ORDER BY played_at, id) AS played_at_seq,
        ROUND(is_skipped_flag / NULLIF(SUM(is_skipped_flag) OVER (PARTITION BY listener_id), 0), 4) AS is_skipped_flag_share_of_listener_id,
        ROUND(AVG(is_skipped_flag) OVER (PARTITION BY track_id, is_skipped), 2) AS track_id_is_skipped_avg_is_skipped_flag,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_track_id_listener_id_by_play)) / 60, 1) AS minutes_since_prev_played_at
    FROM filtered
    WINDOW w_track_id_listener_id_by_play AS (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id), w_track_id_is_skipped AS (PARTITION BY track_id, is_skipped), w_track_id_listener_id_by_play_2 AS (PARTITION BY track_id, listener_id ORDER BY played_at DESC), w_is_skipped_flag_listener_id_3 AS (PARTITION BY is_skipped_flag, listener_id ORDER BY is_skipped_flag, id), w_device_played_at_month_by_pl_2 AS (PARTITION BY device, DATE_TRUNC('MONTH', played_at) ORDER BY played_at ASC, id)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    is_skipped,
    is_skipped_flag,
    second_played_at,
    is_skipped_flag_share_of_track_id_is_skipped,
    is_skipped_flag_change,
    played_at_rank,
    is_skipped_flag_quartile,
    next_is_skipped_flag,
    is_skipped_flag_share_of_listener_id,
    track_id_is_skipped_avg_is_skipped_flag,
    minutes_since_prev_played_at
FROM sequenced
ORDER BY is_skipped_flag DESC
