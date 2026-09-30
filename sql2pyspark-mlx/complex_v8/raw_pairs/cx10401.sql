WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        LENGTH(TRIM(device)) AS device_len,
        YEAR(played_at) AS played_at_year,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE (device NOT IN ('phone', 'speaker') OR TO_DATE(played_at) < DATE '2025-02-18')
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device_len,
        s.played_at_year,
        s.source_slug,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM scoped_plays s
    JOIN plays p ON p.track_id = s.track_id AND p.ms_played >= 49807
)
SELECT *
FROM (
    SELECT listener_id, play_source, device_len
    FROM exploded_detail
) src
PIVOT (
    SUM(device_len)
    FOR play_source IN ('search', 'playlist', 'radio', 'album')
)
ORDER BY listener_id
