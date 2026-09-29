WITH valid_plays AS (
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
    WHERE track_id IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per source, device
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, device ORDER BY ms_played DESC, id DESC) AS rn
        FROM valid_plays
    ) ranked
    WHERE rn = 1
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        CONCAT(device, '#', CAST(track_id AS STRING)) AS device_ref
    FROM latest_per_key
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        device,
        source,
        MIN(ms_played) OVER (PARTITION BY listener_id) AS listener_id_min_ms_played,
        NTILE(4) OVER (PARTITION BY device, track_id ORDER BY ms_played, id) AS ms_played_quartile,
        ROUND(AVG(ms_played) OVER (PARTITION BY device, track_id ORDER BY ms_played, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY device, track_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        MAX(ms_played) OVER (PARTITION BY device, track_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        SUM(ms_played) OVER w_listener_id AS listener_id_sum_ms_played
    FROM derived
    WINDOW w_listener_id AS (PARTITION BY listener_id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    device,
    source,
    listener_id_min_ms_played,
    ms_played_avg_2,
    first_known_ms_played,
    max_ms_played_so_far,
    listener_id_sum_ms_played
FROM windowed
WHERE max_ms_played_so_far < 3201000
ORDER BY max_ms_played_so_far DESC
