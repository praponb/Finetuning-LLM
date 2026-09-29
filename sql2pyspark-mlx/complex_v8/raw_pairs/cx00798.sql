WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'chill')
      AND artist_id <= 9
),
with_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.moods_count,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM cleaned s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        is_explicit,
        artist_country,
        artist_debut_year,
        SIZE(COLLECT_SET(artist_country) OVER w_is_explicit) AS distinct_artist_country_in_is_explicit,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_country, moods_count ORDER BY artist_debut_year ASC, id), 3) AS artist_debut_year_percent_rank,
        FIRST_VALUE(moods_count) IGNORE NULLS OVER (PARTITION BY artist_country, moods_count ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moods_count,
        ROUND(moods_count / NULLIF(SUM(moods_count) OVER (PARTITION BY is_explicit), 0), 4) AS moods_count_share_of_is_explicit,
        ROUND(AVG(moods_count) OVER (PARTITION BY artist_country, moods_count ORDER BY artist_debut_year, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS moods_count_avg_1_prior,
        NTILE(5) OVER w_artist_country_moods_count_b AS artist_debut_year_quintile,
        MAX(artist_debut_year) OVER (PARTITION BY is_explicit) AS is_explicit_max_artist_debut_year
    FROM with_details_artist
    WINDOW w_is_explicit AS (PARTITION BY is_explicit), w_artist_country_moods_count_b AS (PARTITION BY artist_country, moods_count ORDER BY artist_debut_year, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        is_explicit,
        artist_country,
        artist_debut_year,
        distinct_artist_country_in_is_explicit,
        artist_debut_year_percent_rank,
        first_known_moods_count,
        moods_count_share_of_is_explicit,
        moods_count_avg_1_prior,
        is_explicit_max_artist_debut_year,
        moods_count_avg_1_prior - artist_debut_year AS moods_count_avg_1_prior_minus_artist_debut_year,
        ABS(moods_count_avg_1_prior - 1.6) AS moods_count_avg_1_prior_distance
    FROM with_trends
    WHERE artist_debut_year_quintile <= 9
)
-- Final projection
SELECT DISTINCT artist_country, artist_debut_year, artist_debut_year_percent_rank
FROM enriched
ORDER BY artist_debut_year DESC
