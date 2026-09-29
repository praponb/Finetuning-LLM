WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE release_date < DATE '2025-03-29'
      AND is_explicit = FALSE
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.title_slug,
        a.genre AS artist_genre
    FROM valid s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_genre)) AS distinct_artist_id_in_artist_genre,
        NTILE(4) OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec DESC, id) AS duration_sec_quartile,
        MIN(duration_sec) OVER w_artist_genre AS artist_genre_min_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_max_7,
        COUNT(*) OVER (PARTITION BY artist_genre) AS rows_in_artist_genre,
        RANK() OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec DESC, id) AS duration_sec_rank,
        SUM(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_3_prior,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_genre, 2) AS duration_sec_vs_artist_genre_avg,
        DENSE_RANK() OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec DESC, id) AS duration_sec_dense_rank,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_genre, artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec
    FROM combined_details_artist
    WINDOW w_artist_genre AS (PARTITION BY artist_genre)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        title_slug,
        artist_genre,
        distinct_artist_id_in_artist_genre,
        duration_sec_quartile,
        artist_genre_min_duration_sec,
        rows_in_artist_genre,
        duration_sec_sum_3_prior,
        duration_sec_vs_artist_genre_avg,
        ROUND(AVG(duration_sec_dense_rank) OVER (PARTITION BY duration_sec_rank, artist_id), 2) AS duration_sec_rank_artist_id_avg_duration_sec_den,
        SUM(duration_sec_dense_rank) OVER (ORDER BY duration_sec_max_7 DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_dense_rank_sum_4,
        MAX(duration_sec_sum_3_prior) OVER w_duration_sec_rank_artist_id AS duration_sec_rank_artist_id_max_duration_sec_sum,
        duration_sec_dense_rank - LAG(duration_sec_dense_rank) OVER (ORDER BY duration_sec_max_7 DESC, id) AS duration_sec_dense_rank_change,
        NTILE(4) OVER w_all_by_duration_sec_max_7 AS duration_sec_max_7_quartile,
        RANK() OVER (PARTITION BY duration_sec_dense_rank, artist_genre ORDER BY duration_sec_max_7 NULLS LAST) AS duration_sec_max_7_rank,
        SUM(duration_sec_max_7) OVER (PARTITION BY duration_sec_dense_rank, artist_genre ORDER BY duration_sec_max_7 NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_max_7_sum_3_prior
    FROM with_windows
    WINDOW w_duration_sec_rank_artist_id AS (PARTITION BY duration_sec_rank, artist_id), w_all_by_duration_sec_max_7 AS (ORDER BY duration_sec_max_7 DESC, id)
)
SELECT
    id,
    artist_id,
    title_slug,
    artist_genre,
    distinct_artist_id_in_artist_genre,
    duration_sec_quartile,
    artist_genre_min_duration_sec,
    rows_in_artist_genre,
    duration_sec_sum_3_prior,
    duration_sec_rank_artist_id_avg_duration_sec_den,
    duration_sec_rank_artist_id_max_duration_sec_sum,
    duration_sec_dense_rank_change,
    duration_sec_max_7_quartile,
    duration_sec_max_7_rank,
    duration_sec_max_7_sum_3_prior,
    duration_sec_max_7_rank + rows_in_artist_genre AS duration_sec_max_7_rank_plus_rows_in_artist_genr,
    CONCAT_WS(': ', artist_genre, title_slug) AS artist_genre_title_slug_label
FROM ranked
ORDER BY duration_sec_max_7_sum_3_prior NULLS LAST
