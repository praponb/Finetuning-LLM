WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        TRUNC(release_date, 'MM') AS release_date_month_start,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE is_explicit = FALSE
      AND artist_id >= 4
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM recent_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND LOWER(t.title) LIKE '%ou%'
              AND t.duration_sec >= 165
        )
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND ARRAY_CONTAINS(t.moods, 'chill')
        )
),
with_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.release_date_month_start,
        s.is_explicit_flag,
        s.bpm_per_duration_sec,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM filtered s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    artist_id,
    artist_country,
    ROUND(STDDEV(bpm), 2) AS stddev_bpm,
    ROUND(SUM(artist_debut_year) / NULLIF(SUM(duration_sec), 0), 3) AS artist_debut_year_to_duration_sec_ratio,
    ROUND(AVG(is_explicit_flag), 2) AS avg_is_explicit_flag
FROM with_details
GROUP BY GROUPING SETS ((artist_id, artist_country), (artist_id), (artist_country), ())
ORDER BY artist_id NULLS LAST, artist_country NULLS LAST
