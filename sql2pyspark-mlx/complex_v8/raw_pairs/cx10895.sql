WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE bpm < 155
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.moods,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM base_tracks s
    JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        LEAD(artist_debut_year, 2) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date ASC, id) AS next2_artist_debut_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date ASC), 3) AS release_date_percent_rank,
        LEAD(release_date) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date ASC, id) AS next_release_date,
        ROUND(CUME_DIST() OVER w_artist_is_verified_artist_id_3, 3) AS release_date_cume_dist,
        LAG(artist_debut_year) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date DESC, id) AS prev_artist_debut_year,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY MONTH(release_date) ORDER BY artist_debut_year DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date DESC, id), 3) AS release_date_cume_dist_2,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_id), 2) AS artist_debut_year_vs_artist_id_avg,
        DENSE_RANK() OVER w_release_date_month_by_artist_2 AS artist_debut_year_dense_rank,
        DENSE_RANK() OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date DESC) AS release_date_dense_rank,
        LEAD(artist_debut_year) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date ASC, id) AS next_artist_debut_year
    FROM with_details
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_is_verified_artist_id_3 AS (PARTITION BY artist_is_verified, artist_id ORDER BY release_date DESC), w_release_date_month_by_artist_2 AS (PARTITION BY MONTH(release_date) ORDER BY artist_debut_year DESC NULLS FIRST)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        artist_id,
        release_date,
        moods,
        artist_is_verified,
        artist_debut_year,
        rows_in_artist_id,
        next2_artist_debut_year,
        release_date_percent_rank,
        next_release_date,
        release_date_cume_dist,
        prev_artist_debut_year,
        second_artist_debut_year,
        second_release_date,
        release_date_cume_dist_2,
        artist_debut_year_vs_artist_id_avg,
        release_date_dense_rank,
        next_artist_debut_year
    FROM ranked
    WHERE artist_debut_year_dense_rank <= 5
)
SELECT
    id,
    artist_id,
    release_date,
    moods,
    artist_is_verified,
    artist_debut_year,
    next2_artist_debut_year,
    prev_artist_debut_year,
    second_artist_debut_year,
    second_release_date,
    release_date_cume_dist_2,
    artist_debut_year_vs_artist_id_avg,
    release_date_dense_rank,
    next_artist_debut_year
FROM top_per_group
ORDER BY next2_artist_debut_year
