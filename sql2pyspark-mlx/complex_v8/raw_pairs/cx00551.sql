WITH cleaned AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified
    FROM artists
    WHERE LENGTH(TRIM(artist_name)) >= 8
      AND debut_year > 1997
),
qualified AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.bpm < 143
        )
      AND NOT s.is_verified
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(country)) AS country_len,
        LOWER(TRIM(artist_name)) AS artist_name_clean
    FROM qualified
    WHERE debut_year BETWEEN 1994 AND 2018
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS first_track_release_date
    FROM derived s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, country_len, debut_year
    FROM with_counts
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (country_len, debut_year)
) unp
ORDER BY id, metric
