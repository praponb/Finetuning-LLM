WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE bpm > 104
),
unique_rows AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date, id) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
without_matches AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM unique_rows s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country NOT IN ('US', 'KR')
              AND a.genre <> 'Indie'
        )
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        p.id AS play_id,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at
    FROM without_matches s
    JOIN plays p ON p.track_id = s.id
),
filtered_2 AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM exploded_detail s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
combined_details AS (
    -- Attach play attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.play_id,
        s.play_source,
        s.play_is_skipped,
        s.play_ms_played,
        s.play_played_at,
        p.listener_id AS play_listener_id,
        t.is_explicit AS track_is_explicit
    FROM filtered_2 s
    JOIN plays p ON p.id = s.play_id AND p.played_at >= TIMESTAMP '2025-02-06 15:00:00'
    LEFT JOIN tracks t ON t.id = p.track_id
)
SELECT DISTINCT play_is_skipped
FROM combined_details
