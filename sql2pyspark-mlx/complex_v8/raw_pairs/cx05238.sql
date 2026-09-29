WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE (played_at >= TIMESTAMP '2025-01-31 09:00:00' OR ms_played > 95960)
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.track_id AS play_track_id
    FROM cleaned_plays s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM with_children_plays s
    LEFT JOIN play_stats p ON p.track_id = s.play_track_id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        play_id,
        play_track_id,
        LOWER(REGEXP_REPLACE(TRIM(play_device), ' ', '_')) AS play_device_slug,
        LENGTH(TRIM(play_source)) AS play_source_len,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM summarized_rows
),
with_parent_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.play_id,
        s.play_track_id,
        s.play_device_slug,
        s.device_slug,
        t.bpm AS track_bpm,
        t.title AS track_title
    FROM derived s
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    listener_id,
    play_id,
    play_track_id,
    play_device_slug,
    track_title
FROM with_parent_track
WHERE track_title IS NOT NULL
