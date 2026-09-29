WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods
    FROM tracks
    WHERE artist_id <= 5
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY release_date ASC, id), 3) AS release_date_cume_dist,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg
    FROM prepared
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.release_date_cume_dist,
        s.duration_sec_vs_artist_id_avg,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM ranked s
    JOIN plays p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    release_date,
    release_date_cume_dist,
    play_is_skipped,
    DAYOFWEEK(release_date) AS release_date_dayofweek,
    SUBSTRING(TRIM(title), 1, 3) AS title_prefix
FROM line_level_plays
ORDER BY title DESC
