WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        INITCAP(TRIM(title)) AS title_clean
    FROM tracks
    WHERE (is_explicit = TRUE OR bpm > 96)
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped
    FROM valid s
    JOIN plays p ON p.track_id = s.id
)
SELECT
    play_source,
    is_explicit,
    artist_id,
    SUM(duration_sec) AS total_duration_sec,
    SUM(CASE WHEN play_source <> 'radio' THEN 1 ELSE 0 END) AS n_play_source_radio
FROM with_children
GROUP BY GROUPING SETS ((play_source, is_explicit, artist_id), (play_source))
