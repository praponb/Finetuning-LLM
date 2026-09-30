WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        CONCAT(device, '#', CAST(id AS STRING)) AS device_ref,
        INITCAP(TRIM(device)) AS device_clean
    FROM plays
    WHERE track_id >= 25
      AND played_at >= TIMESTAMP '2025-01-25 10:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior,
        ROUND(AVG(ms_played) OVER w_device_track_id, 2) AS device_track_id_avg_ms_played,
        SUM(ms_played) OVER (PARTITION BY device, track_id) AS device_track_id_sum_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER w_device_track_id, 2) AS ms_played_vs_device_track_id_avg
    FROM cleaned
    WINDOW w_device_track_id AS (PARTITION BY device, track_id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT played_at_month, listener_id, source, ms_played_vs_device_track_id_avg
    FROM windowed
) src
PIVOT (
    SUM(ms_played_vs_device_track_id_avg)
    FOR source IN ('search', 'radio', 'album', 'playlist')
)
ORDER BY played_at_month, listener_id
