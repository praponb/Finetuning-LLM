WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE source = 'radio'
      AND listener_id >= 9
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        played_at_label,
        MAX(ms_played) OVER (PARTITION BY device) AS device_max_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY device), 2) AS device_avg_ms_played,
        MAX(ms_played) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_device, 0), 4) AS ms_played_share_of_device,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        RANK() OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id) AS ms_played_rank,
        SUM(ms_played) OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_sum_2,
        SUM(ms_played) OVER (PARTITION BY device) AS device_sum_ms_played,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device)) AS distinct_listener_id_in_device,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        ROUND(CUME_DIST() OVER w_listener_id_by_ms_played_tie, 3) AS ms_played_cume_dist,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device)) AS distinct_track_id_in_device,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at
    FROM prepared_plays
    WINDOW w_device AS (PARTITION BY device), w_listener_id_by_ms_played_tie AS (PARTITION BY listener_id ORDER BY ms_played DESC)
)
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    played_at_label,
    device_max_ms_played,
    device_avg_ms_played,
    ms_played_share_of_device,
    last_known_played_at,
    ms_played_rank,
    distinct_listener_id_in_device,
    rows_in_device,
    ms_played_cume_dist,
    distinct_track_id_in_device,
    second_played_at
FROM sequenced
ORDER BY played_at, ms_played
