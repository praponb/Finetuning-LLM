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
        source,
        LENGTH(TRIM(device)) AS device_len,
        HOUR(played_at) AS played_at_hour,
        DATE_TRUNC('WEEK', played_at) AS played_at_week
    FROM plays
    WHERE ms_played < 326250
      AND is_skipped = TRUE
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        device,
        is_skipped,
        source,
        device_len,
        played_at_hour,
        ROUND(device_len / NULLIF(SUM(device_len) OVER w_listener_id_is_skipped, 0), 4) AS device_len_share_of_listener_id_is_skipped,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_3_prior,
        MAX(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        RANK() OVER (PARTITION BY is_skipped ORDER BY ms_played, id) AS ms_played_rank
    FROM recent
    WINDOW w_listener_id_is_skipped AS (PARTITION BY listener_id, is_skipped)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        played_at_hour + device_len AS played_at_hour_plus_device_len,
        CONCAT_WS(' / ', device, source) AS device_source_label
    FROM windowed
    WHERE source NOT IN ('album', 'radio')
)
SELECT
    id,
    listener_id,
    played_at,
    device,
    is_skipped,
    device_len,
    device_len_share_of_listener_id_is_skipped,
    played_at_hour_plus_device_len,
    device_source_label,
    LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
    played_at + INTERVAL 7 DAYS AS played_at_plus_7d
FROM derived
WHERE max_ms_played_so_far BETWEEN 604250 AND 2801500
