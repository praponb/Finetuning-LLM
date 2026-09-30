WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_ADD(release_date, 7) AS release_date_plus_7d
    FROM tracks
    WHERE duration_sec < 409
      AND NOT is_explicit
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.release_date_plus_7d,
        a.is_verified AS artist_is_verified
    FROM base_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM with_parent_artist s
    JOIN plays p ON p.track_id = s.id
)
SELECT
    play_id,
    artist_id,
    MIN(play_played_at) AS first_play_played_at,
    MIN(play_ms_played) AS min_play_ms_played,
    SUM(play_ms_played) AS total_play_ms_played
FROM with_children_plays
GROUP BY GROUPING SETS ((play_id, artist_id), (play_id), ())
ORDER BY play_id NULLS LAST, artist_id NULLS LAST
