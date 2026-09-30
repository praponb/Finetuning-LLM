WITH recent_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM artists
    WHERE LENGTH(TRIM(artist_name)) >= 5
      AND NOT is_verified
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(debut_year) OVER (PARTITION BY country_or_unknown ORDER BY debut_year, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS debut_year_sum_2_prior,
        MAX(debut_year) OVER (PARTITION BY country_or_unknown ORDER BY debut_year, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS debut_year_max_2,
        SUM(debut_year) OVER w_genre AS genre_sum_debut_year,
        RANK() OVER (PARTITION BY country_or_unknown ORDER BY debut_year, id) AS debut_year_rank
    FROM recent_artists
    WINDOW w_genre AS (PARTITION BY genre)
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, COUNT(*) AS track_count, COUNT(*) AS track_count_2
    FROM tracks
    GROUP BY artist_id
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        COALESCE(t.track_count_2, 0) AS track_count_2
    FROM metrics s
    LEFT JOIN track_stats t ON t.artist_id = s.id
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        country_or_unknown,
        debut_year_sum_2_prior,
        debut_year_max_2,
        genre_sum_debut_year,
        track_count,
        track_count_2
    FROM with_stats
    WHERE debut_year_rank = 1
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS first_track_release_date
    FROM top_per_group s
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        debut_year_max_2,
        genre_sum_debut_year,
        track_count,
        track_count_2,
        first_track_release_date,
        DATE_FORMAT(first_track_release_date, 'yyyy-MM-dd') AS first_track_release_date_label
    FROM profiled
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, track_count, genre_sum_debut_year, track_count_2
    FROM derived
) src
UNPIVOT (
    value FOR metric IN (track_count AS m_track_count, genre_sum_debut_year AS m_genre_sum_debut_year, track_count_2 AS track_count_2_v)
) unp
