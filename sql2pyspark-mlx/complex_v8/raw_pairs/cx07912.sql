WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods
    FROM tracks
    WHERE duration_sec < 392
      AND is_explicit = TRUE
),
tagged AS (
    -- Add calculated columns
    SELECT *, ABS(duration_sec - 352) AS duration_sec_distance
    FROM scoped
),
joined_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.moods,
        s.duration_sec_distance,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM tagged s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.device IS NOT NULL THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_device_count,
        (SELECT COUNT(CASE WHEN p.source <> 'radio' THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_source_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM joined_artist s
)
SELECT *
FROM (
    SELECT artist_id, artist_country, artist_debut_year
    FROM with_stats
) src
PIVOT (
    MIN(artist_debut_year)
    FOR artist_country IN ('TH', 'KR', 'US')
)
