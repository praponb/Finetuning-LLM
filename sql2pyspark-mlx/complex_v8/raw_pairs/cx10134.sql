WITH filtered AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        COALESCE(debut_year, 0) AS debut_year_filled,
        UPPER(TRIM(genre)) AS genre_clean
    FROM artists
    WHERE NOT is_verified
      AND country IS NOT NULL
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) AS track_count,
        MIN(release_date) AS first_track_release_date
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        t.first_track_release_date
    FROM filtered s
    LEFT JOIN track_stats t ON t.artist_id = s.id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_name,
        genre,
        is_verified,
        country_or_unknown,
        debut_year_filled,
        genre_clean,
        track_count,
        MIN(track_count) OVER (PARTITION BY is_verified, MONTH(first_track_release_date)) AS is_verified_first_track_release_date_month_min_t,
        SUM(track_count) OVER (PARTITION BY country_or_unknown ORDER BY track_count DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS track_count_sum_1_prior,
        DENSE_RANK() OVER (PARTITION BY country_or_unknown ORDER BY track_count DESC) AS track_count_dense_rank,
        SUM(debut_year_filled) OVER (PARTITION BY country_or_unknown ORDER BY track_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_debut_year_filled
    FROM profiled
),
with_children_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.artist_name,
        s.genre,
        s.debut_year_filled,
        s.is_verified_first_track_release_date_month_min_t,
        s.track_count_sum_1_prior,
        s.running_debut_year_filled,
        t.id AS track_id,
        t.moods AS track_moods,
        t.title AS track_title
    FROM ranked s
    JOIN tracks t ON t.artist_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(genre, 'ALL') AS genre,
    track_id,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(track_title)) AS track_title_list,
    SUM(track_count_sum_1_prior) AS total_track_count_sum_1_prior,
    ARRAY_SORT(COLLECT_SET(genre)) AS genre_list
FROM with_children_tracks
GROUP BY ROLLUP(genre, track_id)
ORDER BY genre NULLS LAST, track_id NULLS LAST
