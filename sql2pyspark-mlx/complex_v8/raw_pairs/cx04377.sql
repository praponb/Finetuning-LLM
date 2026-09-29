WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket
    FROM artists
    WHERE genre IN ('Pop', 'Classical')
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count
    FROM valid s
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM summarized_rows
    WHERE debut_year_bucket > (SELECT AVG(debut_year_bucket) FROM summarized_rows)
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM with_activity s
    WHERE s.id IN (
            SELECT t.artist_id
            FROM tracks t
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        debut_year_bucket,
        track_count,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER (PARTITION BY country, genre), 0), 4) AS debut_year_share_of_country_genre,
        MAX(track_count) OVER (ORDER BY debut_year_bucket DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS track_count_max_6_prior,
        COUNT(*) OVER (PARTITION BY country, genre) AS rows_in_country_genre,
        NTILE(3) OVER (PARTITION BY country ORDER BY debut_year_bucket, id) AS debut_year_bucket_tercile
    FROM screened
)
SELECT
    debut_year_bucket_tercile,
    genre,
    SUM(track_count_max_6_prior) AS total_track_count_max_6_prior,
    ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
    ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(artist_name)) >= 6 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_artist_name_6
FROM windowed
GROUP BY ROLLUP(debut_year_bucket_tercile, genre)
ORDER BY debut_year_bucket_tercile NULLS LAST, genre NULLS LAST
