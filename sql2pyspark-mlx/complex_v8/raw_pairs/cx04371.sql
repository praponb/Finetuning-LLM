WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        YEAR(release_date) AS release_date_year,
        COALESCE(bpm, 0) AS bpm_filled
    FROM tracks
    WHERE (ARRAY_CONTAINS(moods, 'focus') OR duration_sec >= 239)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.release_date_year,
        s.bpm_filled,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM valid s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY release_date_year, artist_is_verified)) AS distinct_artist_id_in_release_date_year_artist_i,
        LAG(release_date_year) OVER (ORDER BY artist_debut_year NULLS LAST, id) AS prev_release_date_year,
        MAX(release_date_year) OVER (PARTITION BY artist_genre ORDER BY release_date_year ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS release_date_year_max_3_prior,
        NTILE(4) OVER (ORDER BY release_date_year ASC, id) AS release_date_year_quartile,
        LAG(artist_debut_year, 2) OVER (PARTITION BY artist_genre ORDER BY release_date_year ASC, id) AS prev2_artist_debut_year,
        DENSE_RANK() OVER w_all_by_artist_debut_year_tie AS artist_debut_year_dense_rank,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_genre ORDER BY release_date_year ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_4,
        SUM(release_date_year) OVER (ORDER BY release_date_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_release_date_year,
        LEAD(artist_debut_year, 3, 0) OVER (ORDER BY artist_debut_year DESC, id) AS next3_artist_debut_year,
        LAG(release_date) OVER (PARTITION BY release_date_year, artist_id ORDER BY artist_debut_year, id) AS prev_release_date,
        ROUND(bpm_filled - AVG(bpm_filled) OVER (PARTITION BY artist_is_verified, release_date_year), 2) AS bpm_filled_vs_artist_is_verified_release_date_ye,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY artist_genre ORDER BY release_date_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_genre ORDER BY release_date_year ASC, id), 3) AS release_date_year_percent_rank,
        LEAD(release_date_year, 1, 0) OVER (ORDER BY release_date_year ASC, id) AS next_release_date_year,
        MIN(release_date_year) OVER (PARTITION BY release_date_year, artist_id ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_release_date_year_so_far
    FROM combined_details
    WINDOW w_all_by_artist_debut_year_tie AS (ORDER BY artist_debut_year NULLS LAST)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE artist_debut_year_dense_rank <= 3
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    release_date_year,
    bpm_filled,
    artist_debut_year,
    artist_is_verified,
    distinct_artist_id_in_release_date_year_artist_i,
    release_date_year_quartile,
    artist_debut_year_dense_rank,
    duration_sec_avg_4,
    next3_artist_debut_year,
    prev_release_date,
    bpm_filled_vs_artist_is_verified_release_date_ye,
    second_release_date,
    release_date_year_percent_rank,
    next_release_date_year,
    min_release_date_year_so_far,
    ROUND(release_date_year / NULLIF(artist_debut_year, 0), 3) AS release_date_year_per_artist_debut_year
FROM leaders
WHERE duration_sec_avg_4 >= 172.5
ORDER BY release_date_year_quartile DESC, next3_artist_debut_year DESC, id
LIMIT 5
