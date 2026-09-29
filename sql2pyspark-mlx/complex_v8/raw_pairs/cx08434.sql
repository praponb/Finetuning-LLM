WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        UPPER(TRIM(title)) AS title_clean,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref
    FROM tracks
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        title_clean,
        days_since_release_date,
        title_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS rn
        FROM prepared_tracks
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    is_explicit,
    bpm,
    moods,
    title_clean,
    title_ref
FROM latest
ORDER BY id
