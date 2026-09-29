WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE (track_id <= 5 OR HOUR(played_at) BETWEEN 8 AND 13)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source_len,
        LEAST(ms_played, source_len) AS least_ms_played_source_len,
        COALESCE(source_len, 0) AS source_len_filled,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM filtered_plays
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        ms_played,
        device,
        source_len,
        least_ms_played_source_len,
        ms_played_filled,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device, track_id)) AS distinct_listener_id_in_device_track_id,
        MAX(ms_played_filled) OVER (PARTITION BY device, track_id) AS device_track_id_max_ms_played_filled,
        MAX(source_len) OVER (PARTITION BY device, track_id) AS device_track_id_max_source_len
    FROM derived
),
enriched AS (
    -- Attach listener attributes
    SELECT s.*, l.birth_year AS listener_birth_year, l.plan AS listener_plan
    FROM with_trends s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.plan <> 'premium'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, ms_played, device_track_id_max_source_len, distinct_listener_id_in_device_track_id
    FROM enriched
) src
UNPIVOT (
    reading FOR measure IN (ms_played, device_track_id_max_source_len, distinct_listener_id_in_device_track_id)
) unp
ORDER BY id, measure
