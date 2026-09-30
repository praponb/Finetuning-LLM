WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 7) AS release_date_plus_7d
    FROM tracks
    WHERE title IS NOT NULL
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.moods,
        s.release_date_plus_7d,
        a.country AS artist_country,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY MONTH(release_date_plus_7d)), 2) AS bpm_vs_release_date_plus_7d_month_avg,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_country, artist_id ORDER BY bpm, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_2_prior,
        NTILE(10) OVER (PARTITION BY artist_id, artist_genre ORDER BY bpm DESC, id) AS bpm_decile,
        LAG(artist_debut_year, 3) OVER (PARTITION BY artist_genre ORDER BY artist_debut_year ASC NULLS LAST, id) AS prev3_artist_debut_year,
        LAG(bpm, 2) OVER w_release_date_plus_7d_month_a AS prev2_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_genre ORDER BY artist_debut_year ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_2,
        SIZE(COLLECT_SET(artist_country) OVER w_release_date_plus_7d_month) AS distinct_artist_country_in_release_date_plus_7d_,
        MAX(bpm) OVER w_release_date_plus_7d_month AS release_date_plus_7d_month_max_bpm,
        MAX(artist_debut_year) OVER (PARTITION BY artist_id, artist_country) AS artist_id_artist_country_max_artist_debut_year,
        ROUND(AVG(bpm) OVER w_artist_id_artist_country, 2) AS artist_id_artist_country_avg_bpm,
        MAX(bpm) OVER (PARTITION BY artist_genre ORDER BY artist_debut_year ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        LAST_VALUE(bpm) OVER (PARTITION BY artist_id, artist_genre ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        LAST_VALUE(artist_debut_year) OVER (PARTITION BY artist_country ORDER BY artist_debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_debut_year,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_id, artist_country), 2) AS artist_debut_year_vs_artist_id_artist_country_av
    FROM with_details
    WINDOW w_release_date_plus_7d_month_a AS (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_7d), artist_id ORDER BY artist_debut_year DESC NULLS FIRST, id), w_release_date_plus_7d_month AS (PARTITION BY MONTH(release_date_plus_7d)), w_artist_id_artist_country AS (PARTITION BY artist_id, artist_country)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(last_bpm) IGNORE NULLS OVER (PARTITION BY MONTH(release_date_plus_7d), artist_genre ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_bpm,
        NTILE(3) OVER w_release_date_plus_7d_month_a_3 AS artist_debut_year_tercile,
        RANK() OVER w_release_date_plus_7d_month_a_4 AS artist_debut_year_rank,
        COUNT(*) OVER (PARTITION BY distinct_artist_country_in_release_date_plus_7d_, artist_country) AS rows_in_distinct_artist_country_in_release_date_,
        SUM(last_bpm) OVER (ORDER BY bpm_avg_2_prior DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last_bpm_sum_6_prior
    FROM ranked
    WINDOW w_release_date_plus_7d_month_a_3 AS (PARTITION BY MONTH(release_date_plus_7d), artist_genre ORDER BY artist_debut_year DESC, id), w_release_date_plus_7d_month_a_4 AS (PARTITION BY MONTH(release_date_plus_7d), artist_genre ORDER BY artist_debut_year DESC)
)
-- Final projection
SELECT
    id,
    artist_id,
    bpm,
    moods,
    release_date_plus_7d,
    artist_country,
    artist_genre,
    artist_debut_year,
    bpm_avg_2_prior,
    bpm_decile,
    prev3_artist_debut_year,
    prev2_bpm,
    bpm_avg_2,
    distinct_artist_country_in_release_date_plus_7d_,
    max_bpm_so_far,
    last_bpm,
    last_artist_debut_year,
    artist_debut_year_vs_artist_id_artist_country_av,
    first_known_last_bpm,
    artist_debut_year_rank,
    rows_in_distinct_artist_country_in_release_date_,
    last_bpm_sum_6_prior
FROM with_trends
ORDER BY prev2_bpm DESC NULLS FIRST, artist_debut_year_vs_artist_id_artist_country_av DESC, id
LIMIT 5
