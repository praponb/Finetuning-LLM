WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        INITCAP(TRIM(device)) AS device_clean
    FROM plays
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        ms_played,
        device,
        is_skipped,
        ROUND(PERCENT_RANK() OVER w_device_by_ms_played_ties, 3) AS ms_played_percent_rank,
        DENSE_RANK() OVER (PARTITION BY track_id, listener_id ORDER BY ms_played, id) AS ms_played_dense_rank,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY ms_played, id) AS ms_played_dense_rank_2,
        SUM(ms_played) OVER (PARTITION BY is_skipped, device ORDER BY ms_played, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_sum_7,
        DENSE_RANK() OVER (PARTITION BY track_id, listener_id ORDER BY ms_played) AS ms_played_dense_rank_3,
        RANK() OVER (PARTITION BY is_skipped, device ORDER BY ms_played, id) AS ms_played_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS ms_played_vs_listener_id_avg,
        NTILE(5) OVER w_all_by_ms_played AS ms_played_quintile,
        MAX(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_max_3_prior,
        ROUND(AVG(ms_played) OVER w_listener_id, 2) AS listener_id_avg_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY device, track_id), 0), 4) AS ms_played_share_of_device_track_id,
        SUM(ms_played) OVER (PARTITION BY track_id) AS track_id_sum_ms_played
    FROM cleaned
    WINDOW w_device_by_ms_played_ties AS (PARTITION BY device ORDER BY ms_played), w_all_by_ms_played AS (ORDER BY ms_played, id), w_listener_id AS (PARTITION BY listener_id)
),
enriched AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(device), 1, 2) AS device_prefix
    FROM metrics
    WHERE ms_played_sum_7 < 781750
)
SELECT
    id,
    track_id,
    ms_played,
    is_skipped,
    ms_played_percent_rank,
    ms_played_dense_rank,
    ms_played_dense_rank_3,
    ms_played_vs_listener_id_avg,
    ms_played_max_3_prior,
    listener_id_avg_ms_played,
    ms_played_share_of_device_track_id,
    track_id_sum_ms_played,
    device_prefix,
    GREATEST(track_id_sum_ms_played, ms_played_dense_rank_2) AS greatest_track_id_sum_ms_played_ms_played_dense_,
    CONCAT_WS('-', device, device_prefix) AS device_device_prefix_label
FROM enriched
ORDER BY greatest_track_id_sum_ms_played_ms_played_dense_ DESC, device_device_prefix_label NULLS LAST
