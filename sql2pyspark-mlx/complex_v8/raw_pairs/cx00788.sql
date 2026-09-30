WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = TRUE
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped, device ORDER BY ms_played DESC NULLS FIRST, id), 3) AS ms_played_cume_dist,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY is_skipped, device ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ms_played - LAG(ms_played) OVER (ORDER BY ms_played DESC NULLS FIRST, id) AS ms_played_change,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        ROUND(ms_played - AVG(ms_played) OVER w_track_id_device, 2) AS ms_played_vs_track_id_device_avg,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4,
        ROUND(AVG(ms_played) OVER (PARTITION BY device), 2) AS device_avg_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_device, 0), 4) AS ms_played_share_of_device
    FROM cleaned_plays
    WINDOW w_track_id_device AS (PARTITION BY track_id, device), w_device AS (PARTITION BY device)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        ms_played,
        device,
        is_skipped,
        ms_played_cume_dist,
        second_ms_played,
        ms_played_change,
        ms_played_avg_7,
        ms_played_vs_track_id_device_avg,
        ms_played_avg_4,
        device_avg_ms_played,
        ms_played_share_of_device,
        NTH_VALUE(ms_played_avg_4, 2) OVER (ORDER BY ms_played_vs_track_id_device_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_avg_4,
        RANK() OVER (ORDER BY ms_played_cume_dist) AS ms_played_cume_dist_rank,
        LAST_VALUE(ms_played_avg_4) IGNORE NULLS OVER (PARTITION BY is_skipped, listener_id ORDER BY device_avg_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_avg_4,
        LAST_VALUE(ms_played_avg_4) OVER (PARTITION BY track_id ORDER BY ms_played_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_avg_4
    FROM metrics
)
-- Final projection
SELECT DISTINCT second_ms_played, last_known_ms_played_avg_4, ms_played_avg_7
FROM with_trends
WHERE device IS NOT NULL
ORDER BY last_known_ms_played_avg_4, second_ms_played
