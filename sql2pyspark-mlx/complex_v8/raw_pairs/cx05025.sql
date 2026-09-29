WITH recent AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE title IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        NTILE(3) OVER w_is_explicit_by_duration_sec AS duration_sec_tercile,
        RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC) AS duration_sec_rank,
        MAX(duration_sec) OVER w_is_explicit AS is_explicit_max_duration_sec
    FROM recent
    WINDOW w_is_explicit_by_duration_sec AS (PARTITION BY is_explicit ORDER BY duration_sec DESC, id), w_is_explicit AS (PARTITION BY is_explicit)
),
with_parent_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.moods,
        s.duration_sec_tercile,
        s.duration_sec_rank,
        s.is_explicit_max_duration_sec,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM windowed s
    JOIN artists a ON a.id = s.artist_id
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.moods,
        s.duration_sec_rank,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM with_parent_artist s
    LEFT JOIN plays p ON p.track_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric_name, reading
FROM (
    SELECT id, play_id, duration_sec_rank, play_ms_played
    FROM detail_rows
) src
UNPIVOT (
    reading FOR metric_name IN (duration_sec_rank, play_ms_played)
) unp
