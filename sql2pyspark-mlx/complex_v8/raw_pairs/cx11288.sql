WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        SUBSTRING(TRIM(genre), 1, 3) AS genre_prefix,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        SUBSTRING(TRIM(country), 1, 4) AS country_prefix
    FROM artists
    WHERE debut_year > 1996
      AND genre = 'HipHop'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN is_verified_flag >= 1 THEN 'A' WHEN is_verified_flag >= 0 THEN 'B' ELSE 'C' END AS is_verified_flag_tier
    FROM prepared
    WHERE is_verified_flag < 1
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
        )
      AND s.is_verified_flag_tier NOT IN ('A', 'B')
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS track_is_explicit_count
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count
    FROM without_matches s
    LEFT JOIN track_stats t ON t.artist_id = s.id
)
SELECT id, metric_name, amount
FROM (
    SELECT id, track_is_explicit_count, is_verified_flag, debut_year
    FROM with_counts
) src
UNPIVOT (
    amount FOR metric_name IN (track_is_explicit_count, is_verified_flag, debut_year)
) unp
ORDER BY id, metric_name
