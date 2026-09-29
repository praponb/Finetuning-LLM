WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE listener_id <= 8
),
enriched_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        t.moods AS track_moods,
        t.artist_id AS track_artist_id
    FROM filtered_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix,
        CONCAT(device, '#', CAST(id AS STRING)) AS device_ref,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM enriched_track
    WHERE SIZE(track_moods) >= 1
),
summary AS (
    -- Summarise per group
    SELECT
        source,
        track_id,
        SUM(CASE WHEN device = 'car' THEN 1 ELSE 0 END) AS n_device_car,
        COUNT(*) FILTER (WHERE source <> 'radio') AS n_source_radio,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        COUNT(*) AS n_rows
    FROM enriched
    GROUP BY source, track_id
)
SELECT DISTINCT source, n_rows
FROM summary
ORDER BY source DESC
