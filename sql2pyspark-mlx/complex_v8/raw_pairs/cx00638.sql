WITH cleaned AS (
    -- Filter artists
    SELECT
        id,
        genre,
        country,
        debut_year,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE country IN ('US', 'JP')
      AND debut_year > 1997
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        genre,
        debut_year,
        debut_year_filled,
        LENGTH(TRIM(country)) AS country_len
    FROM cleaned
    WHERE genre <> 'HipHop'
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(CASE WHEN is_explicit = TRUE THEN 1 END) AS track_is_explicit_count
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count
    FROM tagged s
    LEFT JOIN track_stats t ON t.artist_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT country_len, track_is_explicit_count, genre, debut_year
    FROM summarized_rows
) src
PIVOT (
    MAX(debut_year) AS max_v, SUM(debut_year) AS sum_v
    FOR genre IN ('Pop', 'Classical')
)
ORDER BY country_len, track_is_explicit_count
