WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        LENGTH(TRIM(source)) AS source_len,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-13 12:00:00'
      AND device IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(source_len) OVER (ORDER BY days_since_played_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS source_len_max_6_prior,
        NTILE(3) OVER w_all_by_days_since_played_at_2 AS days_since_played_at_tercile,
        SUM(ms_played) OVER w_source_len_device AS source_len_device_sum_ms_played,
        LAG(ms_played) OVER (ORDER BY days_since_played_at DESC, id) AS prev_ms_played
    FROM prepared
    WINDOW w_all_by_days_since_played_at_2 AS (ORDER BY days_since_played_at DESC, id), w_source_len_device AS (PARTITION BY source_len, device)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        days_since_played_at_tercile,
        prev_ms_played,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix,
        CONCAT(device, '#', CAST(prev_ms_played AS STRING)) AS device_ref
    FROM ranked
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device_prefix,
        l.country AS listener_country
    FROM annotated s
    JOIN listeners l ON l.id = s.listener_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(listener_country)) AS listener_country_clean,
        LENGTH(TRIM(listener_country)) AS listener_country_len,
        SUBSTRING(TRIM(device_prefix), 1, 2) AS device_prefix_prefix
    FROM with_parent_listener
)
-- Final projection
SELECT
    id,
    listener_id,
    device_prefix,
    listener_country_clean,
    device_prefix_prefix
FROM tagged
WHERE listener_country_len >= 6
ORDER BY id DESC
