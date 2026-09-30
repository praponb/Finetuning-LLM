WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        duration_sec + bpm AS duration_sec_plus_bpm,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'focus')
      AND is_explicit = TRUE
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.release_date_label,
        s.title_len,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at,
        p.source AS play_source
    FROM cleaned_tracks s
    JOIN plays p ON p.track_id = s.id AND p.source = 'search'
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.play_id,
        s.play_listener_id,
        s.play_ms_played,
        s.play_played_at,
        s.play_source,
        l.interests AS listener_interests,
        p.ms_played AS play_ms_played_2
    FROM line_level s
    LEFT JOIN listeners l ON l.id = s.play_listener_id
    LEFT JOIN plays p ON p.id = s.play_id
),
with_parent_play AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.play_id,
        s.play_listener_id,
        s.play_ms_played,
        s.play_played_at,
        s.listener_interests,
        p.track_id AS play_track_id
    FROM combined_details_listener s
    LEFT JOIN plays p ON p.id = s.play_id
),
deduped AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        play_id,
        play_listener_id,
        play_ms_played,
        play_played_at,
        listener_interests,
        play_track_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY play_ms_played, id, play_id) AS row_num
        FROM with_parent_play
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    play_ms_played,
    play_played_at
FROM deduped
ORDER BY play_played_at DESC
