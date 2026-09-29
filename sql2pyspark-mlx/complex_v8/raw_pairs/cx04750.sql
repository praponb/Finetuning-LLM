WITH prepared AS (
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
        CONCAT(device, '#', CAST(id AS STRING)) AS device_ref,
        CONCAT_WS(': ', source, device) AS source_device_label
    FROM plays
),
totals AS (
    -- Group-level totals
    SELECT
        source,
        track_id,
        SUM(CASE WHEN device NOT IN ('tv', 'car') THEN 1 ELSE 0 END) AS n_device_not_in_tv_car,
        SUM(CASE WHEN device IS NOT NULL THEN 1 ELSE 0 END) AS n_device_is_not_null,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM prepared
    WHERE ms_played >= 73335
    GROUP BY 1, 2
),
joined AS (
    -- Attach track attributes
    SELECT
        s.source,
        s.track_id,
        s.n_device_not_in_tv_car,
        s.n_device_is_not_null,
        s.median_ms_played,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        t.artist_id AS track_artist_id,
        a.debut_year AS artist_debut_year
    FROM totals s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
combined_details AS (
    -- Attach track attributes
    SELECT
        s.source,
        s.track_id,
        s.n_device_not_in_tv_car,
        s.n_device_is_not_null,
        s.median_ms_played,
        s.track_artist_id,
        s.artist_debut_year,
        t.artist_id AS track_artist_id_2
    FROM joined s
    LEFT JOIN tracks t ON t.id = s.track_id AND t.duration_sec >= 154
)
SELECT source, track_id, metric, metric_value
FROM (
    SELECT source, track_id, n_device_is_not_null, n_device_not_in_tv_car, median_ms_played
    FROM combined_details
) src
UNPIVOT (
    metric_value FOR metric IN (n_device_is_not_null, n_device_not_in_tv_car, median_ms_played)
) unp
ORDER BY source, track_id, metric
