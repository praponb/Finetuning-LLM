WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        played_at + INTERVAL 30 MINUTES AS played_at_plus_30m,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE source <> 'search'
      AND track_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        track_id,
        ms_played,
        is_skipped,
        source,
        days_since_played_at,
        FLOOR(days_since_played_at / 100) * 100 AS days_since_played_at_bucket,
        LENGTH(TRIM(source)) AS source_len,
        CASE WHEN ms_played >= 225733 THEN 'large' WHEN ms_played >= 99376 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM recent_plays
    WHERE NOT is_skipped
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.track_id,
        s.source,
        s.days_since_played_at,
        s.days_since_played_at_bucket,
        s.source_len,
        s.ms_played_tier,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        t.artist_id AS track_artist_id
    FROM enriched s
    JOIN tracks t ON t.id = s.track_id
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM with_parent s
    WHERE s.track_artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.title IS NOT NULL
        )
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.track_id,
        s.source,
        s.days_since_played_at_bucket,
        s.track_duration_sec,
        s.track_artist_id,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM qualified s
    JOIN plays p ON p.track_id = s.track_id
)
-- Final projection
SELECT
    id,
    days_since_played_at_bucket,
    play_id,
    play_listener_id,
    play_is_skipped,
    SUBSTRING(TRIM(source), 1, 2) AS source_prefix,
    DATEDIFF(DATE '2026-01-01', play_played_at) AS days_since_play_played_at
FROM exploded_detail
ORDER BY play_listener_id
