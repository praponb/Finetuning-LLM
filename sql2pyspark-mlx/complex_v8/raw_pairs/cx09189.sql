WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE listener_id IS NOT NULL
      AND device IN ('phone', 'desktop')
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.is_skipped = FALSE
        )
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM screened
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        is_skipped,
        device_ref,
        ms_played_bucket,
        NTILE(4) OVER (PARTITION BY is_skipped ORDER BY ms_played_bucket DESC, id) AS ms_played_bucket_quartile,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY is_skipped), 2) AS ms_played_vs_is_skipped_avg
    FROM annotated
)
SELECT
    listener_id,
    ms_played_bucket_quartile,
    SUM(ms_played_bucket) AS total_ms_played_bucket
FROM metrics
GROUP BY GROUPING SETS ((listener_id, ms_played_bucket_quartile), (listener_id), ())
