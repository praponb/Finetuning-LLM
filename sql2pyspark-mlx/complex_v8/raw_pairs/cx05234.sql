WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-09-26' AND DATE '2025-08-28'
      AND artist_id >= 7
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.*,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM prepared_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        CASE WHEN bpm >= 163 THEN 'HIGH' WHEN bpm >= 100 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier,
        artist_debut_year - duration_sec AS artist_debut_year_minus_duration_sec
    FROM combined_details_artist
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, duration_sec, artist_debut_year_minus_duration_sec
    FROM scored
) src
UNPIVOT (
    value FOR metric_name IN (duration_sec, artist_debut_year_minus_duration_sec)
) unp
ORDER BY id, metric_name
