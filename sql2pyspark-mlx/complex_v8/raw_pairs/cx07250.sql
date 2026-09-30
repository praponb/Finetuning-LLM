WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE source <> 'search'
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        played_at_plus_1d,
        played_at_label,
        CONCAT(played_at_label, '#', CAST(listener_id AS STRING)) AS played_at_label_ref,
        DATE_TRUNC('QUARTER', played_at_date) AS played_at_date_quarter,
        SUBSTRING(TRIM(played_at_label), 1, 4) AS played_at_label_prefix
    FROM prepared
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.played_at_label,
        s.played_at_label_ref,
        t.title AS track_title,
        t.is_explicit AS track_is_explicit,
        t.duration_sec AS track_duration_sec,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM annotated s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
    WHERE t.duration_sec < 312
)
-- Final projection
SELECT
    id,
    listener_id,
    is_skipped,
    source,
    played_at_label,
    played_at_label_ref,
    track_title,
    track_duration_sec,
    artist_debut_year,
    artist_name,
    LENGTH(TRIM(played_at_label_ref)) AS played_at_label_ref_len
FROM with_details
WHERE played_at_label IS NOT NULL
