WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
        CONCAT_WS('-', source, device) AS source_device_label,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE device IN ('speaker', 'phone')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        device_prefix,
        source_device_label,
        source_clean,
        SUM(ms_played) OVER (PARTITION BY track_id, device ORDER BY ms_played, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_sum_2_prior,
        MIN(ms_played) OVER (PARTITION BY device) AS device_min_ms_played
    FROM recent_plays
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(device_min_ms_played, ms_played_sum_2_prior) AS greatest_device_min_ms_played_ms_played_sum_2_pr,
        device_min_ms_played - ms_played AS device_min_ms_played_minus_ms_played
    FROM with_windows
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, ms_played_sum_2_prior, device_min_ms_played, device_min_ms_played_minus_ms_played, ms_played
    FROM derived
) src
UNPIVOT (
    amount FOR measure IN (ms_played_sum_2_prior, device_min_ms_played, device_min_ms_played_minus_ms_played, ms_played)
) unp
