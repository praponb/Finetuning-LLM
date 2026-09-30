WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit
    FROM tracks
    WHERE bpm < 142
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DAYOFWEEK(release_date) AS release_date_dayofweek,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM prepared
    WHERE release_date < DATE '2025-08-02'
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.duration_sec_filled,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM enriched s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.is_verified = FALSE
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, duration_sec, artist_debut_year
    FROM with_details_artist
) src
UNPIVOT (
    value FOR metric_name IN (duration_sec AS m_duration_sec, artist_debut_year AS m_artist_debut_year)
) unp
