WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(TRIM(title)) AS title_clean,
        GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec,
        DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM tracks
    WHERE artist_id >= 6
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(release_date_dayofweek - AVG(release_date_dayofweek) OVER (PARTITION BY artist_id, MONTH(release_date)), 2) AS release_date_dayofweek_vs_artist_id_release_date,
        ROUND(release_date_dayofweek / NULLIF(SUM(release_date_dayofweek) OVER (PARTITION BY artist_id, MONTH(release_date)), 0), 4) AS release_date_dayofweek_share_of_artist_id_releas
    FROM prepared
),
enriched_artist AS (
    -- Join artists
    SELECT
        s.*,
        a.artist_name,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM with_trends s
    JOIN artists a ON a.id = s.artist_id
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.moods,
        s.release_date_dayofweek,
        s.release_date_dayofweek_share_of_artist_id_releas,
        s.artist_name,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped,
        p.listener_id AS play_listener_id
    FROM enriched_artist s
    JOIN plays p ON p.track_id = s.id AND p.played_at >= TIMESTAMP '2025-02-06 12:00:00'
)
SELECT
    id,
    artist_id,
    bpm,
    moods,
    release_date_dayofweek_share_of_artist_id_releas,
    artist_name,
    play_played_at,
    play_ms_played,
    play_is_skipped,
    play_listener_id,
    ARRAY_CONTAINS(moods, 'focus') AS has_focus
FROM line_level_plays
ORDER BY bpm DESC, play_listener_id DESC
