WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-12 10:00:00'
      AND is_skipped = FALSE
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.played_at_date,
        p.id AS play_id,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM scoped_plays s
    JOIN plays p ON p.track_id = s.track_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(play_ms_played, 0) AS play_ms_played_filled,
        CASE WHEN play_source IN ('playlist', 'album') THEN 'group_a' WHEN play_source = 'search' THEN 'group_b' ELSE 'other' END AS play_source_group
    FROM with_children_plays
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    played_at_date,
    play_id,
    play_device,
    play_played_at,
    play_source,
    play_ms_played,
    play_ms_played_filled
FROM scored
ORDER BY play_played_at DESC, play_id DESC
