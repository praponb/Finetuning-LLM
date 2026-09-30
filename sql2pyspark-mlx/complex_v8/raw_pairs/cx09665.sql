WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-12'
),
combined_details_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        t.title AS track_title,
        a.artist_name
    FROM prepared s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
combined_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.artist_name,
        t.moods AS track_moods,
        t.artist_id AS track_artist_id
    FROM combined_details_track s
    JOIN tracks t ON t.id = s.track_id AND NOT t.is_explicit
    WHERE ARRAY_CONTAINS(t.moods, 'focus')
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    is_skipped,
    artist_name,
    track_moods,
    track_artist_id,
    SUBSTRING(TRIM(artist_name), 1, 4) AS artist_name_prefix,
    CASE WHEN ms_played >= 236512 THEN 'A' WHEN ms_played >= 130888 THEN 'B' ELSE 'C' END AS ms_played_tier
FROM combined_details
