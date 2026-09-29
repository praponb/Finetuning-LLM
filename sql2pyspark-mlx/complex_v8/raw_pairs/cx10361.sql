WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        LENGTH(TRIM(title)) AS title_len,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.release_date_label,
        s.title_len,
        s.title_clean,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country
    FROM filtered s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT id, metric, metric_value
FROM (
    SELECT id, title_len, bpm, duration_sec
    FROM enriched_artist
) src
UNPIVOT (
    metric_value FOR metric IN (title_len, bpm, duration_sec)
) unp
