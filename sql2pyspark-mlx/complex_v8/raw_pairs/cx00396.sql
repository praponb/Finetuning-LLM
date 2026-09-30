WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.debut_year AS artist_debut_year
    FROM prepared s
    LEFT JOIN artists a ON a.id = s.artist_id
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.debut_year AS artist_debut_year_2
    FROM joined s
    JOIN artists a ON a.id = s.artist_id AND a.genre NOT IN ('Pop', 'Classical')
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM combined_details_artist s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id IS NOT NULL
              AND p.ms_played > 52009
        )
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played,
        p.source AS play_source,
        p.device AS play_device
    FROM without_matches s
    JOIN plays p ON p.track_id = s.id AND p.source = 'album'
)
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    moods,
    play_id,
    play_played_at,
    play_ms_played,
    play_source,
    play_device,
    play_played_at + INTERVAL 30 MINUTES AS play_played_at_plus_30m,
    GREATEST(bpm, play_ms_played) AS greatest_bpm_play_ms_played
FROM detail_rows_plays
ORDER BY greatest_bpm_play_ms_played DESC
