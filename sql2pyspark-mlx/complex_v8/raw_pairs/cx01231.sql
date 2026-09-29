WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE (release_date BETWEEN DATE '2024-06-03' AND DATE '2025-08-02' OR bpm < 127)
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.moods,
        p.id AS play_id,
        p.source AS play_source,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM prepared s
    LEFT JOIN plays p ON p.track_id = s.id
)
-- Subtotals with CUBE
SELECT
    play_id,
    artist_id,
    play_source,
    MIN(release_date) AS first_release_date,
    COUNT(DISTINCT play_id) AS distinct_play_ids,
    ARRAY_SORT(COLLECT_SET(play_source)) AS play_source_list
FROM exploded_detail_plays
GROUP BY CUBE(play_id, artist_id, play_source)
