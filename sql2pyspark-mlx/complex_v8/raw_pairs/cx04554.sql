WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND title IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.title_slug,
        a.country AS artist_country,
        a.artist_name,
        a.is_verified AS artist_is_verified
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.duration_sec) FROM tracks t WHERE t.artist_id = s.artist_id) AS min_track_duration_sec,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date,
        (SELECT SUM(t.bpm) FROM tracks t WHERE t.artist_id = s.artist_id) AS total_track_bpm
    FROM joined s
)
SELECT *
FROM (
    SELECT last_track_release_date, artist_country, total_track_bpm
    FROM with_counts
) src
PIVOT (
    SUM(total_track_bpm)
    FOR artist_country IN ('JP', 'TH')
)
ORDER BY last_track_release_date
