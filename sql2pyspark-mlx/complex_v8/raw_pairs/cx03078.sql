WITH base_plays AS (
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
        CASE WHEN device IN ('desktop', 'phone') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE MONTH(played_at) IN (1, 2)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ms_played + ms_played_bucket AS ms_played_plus_ms_played_bucket,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix,
        played_at + INTERVAL 30 MINUTES AS played_at_plus_30m
    FROM base_plays
),
latest_per_key AS (
    -- Keep the latest row per track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        device_group,
        ms_played_bucket,
        ms_played_plus_ms_played_bucket,
        device_prefix,
        played_at_plus_30m
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY ms_played_plus_ms_played_bucket DESC, id DESC) AS row_num
        FROM tagged
    ) ranked
    WHERE row_num = 1
)
SELECT
    source,
    track_id,
    MIN(played_at) AS first_played_at,
    COUNT(*) AS n_rows
FROM latest_per_key
GROUP BY ROLLUP(source, track_id)
ORDER BY source NULLS LAST, track_id NULLS LAST
