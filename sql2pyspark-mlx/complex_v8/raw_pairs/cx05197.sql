WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE ms_played >= 99376
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.source,
        p.id AS play_id,
        p.source AS play_source,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.listener_id AS play_listener_id
    FROM cleaned_plays s
    LEFT JOIN plays p ON p.track_id = s.track_id
)
SELECT
    id,
    listener_id,
    track_id,
    source,
    play_id,
    play_is_skipped,
    play_listener_id,
    CONCAT_WS('|', source, play_source) AS source_play_source_label,
    LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
FROM exploded_detail
ORDER BY source_slug NULLS LAST, source
