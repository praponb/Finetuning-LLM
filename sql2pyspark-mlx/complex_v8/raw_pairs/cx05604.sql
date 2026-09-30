WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
    WHERE YEAR(release_date) = 2025
      AND NOT ARRAY_CONTAINS(moods, 'sad')
),
grouped AS (
    -- Summarise per group
    SELECT
        YEAR(release_date) AS release_date_year,
        artist_id,
        is_explicit,
        PERCENTILE_APPROX(bpm, 0.5) AS median_bpm,
        ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec,
        COUNT(*) FILTER (WHERE NOT is_explicit) AS n_not_is_explicit,
        SUM(bpm) AS total_bpm
    FROM base_tracks
    GROUP BY YEAR(release_date), artist_id, is_explicit
    HAVING SUM(duration_sec) > 184
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.release_date_year,
        s.artist_id,
        s.is_explicit,
        s.median_bpm,
        s.stddev_duration_sec,
        s.n_not_is_explicit,
        a.artist_name,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM grouped s
    JOIN artists a ON a.id = s.artist_id
),
with_activity AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM with_details_artist s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.is_verified = FALSE
              AND a.debut_year >= 1996
        )
)
SELECT release_date_year, artist_id, field, reading
FROM (
    SELECT release_date_year, artist_id, n_not_is_explicit, median_bpm
    FROM with_activity
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (n_not_is_explicit AS m_n_not_is_explicit, median_bpm AS median_bpm_v)
) unp
