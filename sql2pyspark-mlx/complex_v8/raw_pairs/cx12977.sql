WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ABS(duration_sec - 213) AS duration_sec_distance
    FROM tracks
    WHERE SIZE(moods) >= 2
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM scoped
    WHERE bpm > (SELECT MIN(bpm) FROM scoped)
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.duration_sec_distance,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped,
        p.listener_id AS play_listener_id
    FROM without_matches s
    JOIN plays p ON p.track_id = s.id
)
SELECT
    id,
    artist_id,
    duration_sec,
    duration_sec_distance,
    play_id,
    play_ms_played,
    play_listener_id,
    COALESCE(play_ms_played, 0) AS play_ms_played_filled
FROM detail_rows_plays
WHERE play_ms_played BETWEEN 49807 AND 239955
ORDER BY id DESC NULLS FIRST, play_listener_id DESC
