WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = TRUE
      AND ms_played >= 71476
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id, source)) AS distinct_track_id_in_listener_id_source,
        LAG(ms_played) OVER (PARTITION BY source ORDER BY played_at ASC, id) AS prev_ms_played,
        NTILE(4) OVER (PARTITION BY YEAR(played_at), listener_id ORDER BY played_at DESC, id) AS played_at_quartile,
        ms_played - LAG(ms_played) OVER w_played_at_year_listener_id_b AS ms_played_change,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped ORDER BY played_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4,
        MIN(ms_played) OVER (PARTITION BY is_skipped, device) AS is_skipped_device_min_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY YEAR(played_at), listener_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        LAG(ms_played, 3) OVER (PARTITION BY is_skipped ORDER BY played_at DESC, id) AS prev3_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY is_skipped, device), 0), 4) AS ms_played_share_of_is_skipped_device,
        LEAD(ms_played) OVER w_played_at_year_listener_id_b AS next_ms_played,
        RANK() OVER (ORDER BY ms_played DESC) AS ms_played_rank,
        SUM(ms_played) OVER (PARTITION BY is_skipped, device) AS is_skipped_device_sum_ms_played,
        MAX(ms_played) OVER w_is_skipped_device AS is_skipped_device_max_ms_played
    FROM recent
    WINDOW w_played_at_year_listener_id_b AS (PARTITION BY YEAR(played_at), listener_id ORDER BY played_at DESC, id), w_is_skipped_device AS (PARTITION BY is_skipped, device)
)
-- Final projection
SELECT
    id,
    played_at,
    ms_played,
    source,
    distinct_track_id_in_listener_id_source,
    prev_ms_played,
    played_at_quartile,
    ms_played_change,
    ms_played_avg_4,
    is_skipped_device_min_ms_played,
    last_known_ms_played,
    prev3_ms_played,
    ms_played_rank,
    INITCAP(TRIM(device)) AS device_clean
FROM windowed
ORDER BY device_clean
