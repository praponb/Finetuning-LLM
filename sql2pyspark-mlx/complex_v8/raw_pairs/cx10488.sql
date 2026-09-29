WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-18'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        t.moods AS track_moods,
        t.title AS track_title,
        a.artist_name
    FROM recent s
    LEFT JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        artist_name,
        CONCAT(artist_name, '#', CAST(id AS STRING)) AS artist_name_ref,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref,
        CONCAT_WS('-', track_title, artist_name) AS track_title_artist_name_label
    FROM combined_details
)
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    artist_name,
    source_ref,
    track_title_artist_name_label
FROM scored
WHERE track_id <= 10
ORDER BY id DESC, track_title_artist_name_label
