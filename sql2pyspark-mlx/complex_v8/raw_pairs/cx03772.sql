WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tracks
    WHERE is_explicit = TRUE
      AND SIZE(moods) >= 1
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        a.genre AS artist_genre,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM filtered s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.genre NOT IN ('Pop', 'HipHop')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        artist_name,
        artist_debut_year,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id ORDER BY artist_debut_year DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_3,
        ROW_NUMBER() OVER (ORDER BY release_date NULLS LAST, id) AS rn,
        bpm - LAG(bpm) OVER w_artist_genre_by_release_date AS bpm_change,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_genre), 2) AS artist_debut_year_vs_artist_genre_avg,
        NTH_VALUE(bpm, 2) OVER (ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY artist_debut_year DESC NULLS FIRST), 3) AS artist_debut_year_cume_dist,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY artist_debut_year DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS artist_debut_year_avg_2_prior,
        SUM(bpm) OVER (PARTITION BY artist_genre) AS artist_genre_sum_bpm,
        MIN(artist_debut_year) OVER (PARTITION BY artist_genre) AS artist_genre_min_artist_debut_year,
        ROUND(AVG(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_genre ORDER BY bpm DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_3_2,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_genre)) AS distinct_artist_id_in_artist_genre
    FROM combined_details
    WINDOW w_artist_genre_by_release_date AS (PARTITION BY artist_genre ORDER BY release_date, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        bpm_change,
        artist_debut_year_vs_artist_genre_avg,
        artist_debut_year_cume_dist,
        artist_debut_year_avg_2_prior,
        artist_genre_sum_bpm,
        bpm_avg_3_2,
        ROUND(AVG(second_bpm) OVER (PARTITION BY rn ORDER BY artist_debut_year_cume_dist DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS second_bpm_avg_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY rn ORDER BY bpm_change, id), 3) AS bpm_change_percent_rank,
        NTILE(10) OVER w_distinct_artist_id_in_artist AS artist_debut_year_cume_dist_decile,
        bpm_change - LAG(bpm_change) OVER (PARTITION BY rn ORDER BY bpm_change, id) AS bpm_change_change
    FROM windowed
    WINDOW w_distinct_artist_id_in_artist AS (PARTITION BY distinct_artist_id_in_artist_genre, artist_id ORDER BY artist_debut_year_cume_dist, id)
)
SELECT
    id,
    artist_id,
    title,
    bpm,
    moods,
    bpm_change,
    artist_debut_year_vs_artist_genre_avg,
    artist_debut_year_cume_dist,
    artist_debut_year_avg_2_prior,
    artist_genre_sum_bpm,
    second_bpm_avg_7,
    artist_debut_year_cume_dist_decile,
    bpm_change_change
FROM with_windows
WHERE release_date >= DATE '2024-06-03'
ORDER BY second_bpm_avg_7, title
