WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        LENGTH(TRIM(source)) AS source_len,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM plays
    WHERE device = 'speaker'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source_len,
        device_prefix,
        FIRST_VALUE(source_len) OVER (PARTITION BY TO_DATE(played_at), track_id ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_source_len,
        ms_played - LAG(ms_played) OVER (ORDER BY ms_played DESC NULLS FIRST, id) AS ms_played_change,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        MAX(source_len) OVER (PARTITION BY listener_id ORDER BY ms_played ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS source_len_max_7,
        FIRST_VALUE(played_at) OVER (PARTITION BY listener_id ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        NTILE(4) OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id) AS ms_played_quartile,
        MIN(source_len) OVER (PARTITION BY listener_id ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_source_len_so_far,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        NTILE(4) OVER (ORDER BY ms_played DESC, id) AS ms_played_quartile_2,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        LAG(played_at, 3) OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id) AS prev3_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4
    FROM cleaned_plays
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    device_prefix,
    ms_played_change,
    second_ms_played,
    source_len_max_7,
    ms_played_quartile,
    min_source_len_so_far,
    last_known_ms_played,
    ms_played_quartile_2,
    rows_in_listener_id,
    prev3_played_at,
    ms_played_avg_4,
    SUBSTRING(TRIM(device_prefix), 1, 4) AS device_prefix_prefix,
    SUBSTRING(TRIM(device_prefix), 1, 2) AS device_prefix_prefix_2
FROM windowed
WHERE last_known_ms_played >= 112786
ORDER BY second_ms_played
