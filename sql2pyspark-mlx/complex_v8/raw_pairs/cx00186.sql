WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE title IS NOT NULL
),
with_parent_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods,
        a.artist_name,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM base_tracks s
    JOIN artists a ON a.id = s.artist_id AND a.genre = 'HipHop'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        artist_name,
        artist_country,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_genre)) AS distinct_artist_id_in_artist_genre,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_genre, artist_country ORDER BY bpm DESC), 3) AS bpm_cume_dist,
        SUM(bpm) OVER (PARTITION BY artist_genre) AS artist_genre_sum_bpm,
        bpm - LAG(bpm) OVER (PARTITION BY artist_genre, artist_country ORDER BY bpm DESC, id) AS bpm_change,
        SUM(bpm) OVER (PARTITION BY artist_country ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_sum_6_prior,
        MIN(duration_sec) OVER (PARTITION BY artist_country ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        LAST_VALUE(duration_sec) OVER (PARTITION BY artist_country ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        SUM(duration_sec) OVER w_artist_genre AS artist_genre_sum_duration_sec,
        ROUND(bpm - AVG(bpm) OVER w_artist_genre, 2) AS bpm_vs_artist_genre_avg,
        RANK() OVER w_artist_genre_artist_country AS bpm_rank,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_genre, 0), 4) AS duration_sec_share_of_artist_genre,
        ROW_NUMBER() OVER w_artist_country_by_duration_s_4 AS duration_sec_seq,
        MAX(duration_sec) OVER (PARTITION BY artist_country ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_max_3_prior,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_genre), 2) AS duration_sec_vs_artist_genre_avg,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_genre, artist_country ORDER BY bpm DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior
    FROM with_parent_artist
    WINDOW w_artist_genre AS (PARTITION BY artist_genre), w_artist_genre_artist_country AS (PARTITION BY artist_genre, artist_country ORDER BY bpm DESC), w_artist_country_by_duration_s_4 AS (PARTITION BY artist_country ORDER BY duration_sec, id)
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        artist_name,
        artist_country,
        distinct_artist_id_in_artist_genre,
        bpm_cume_dist,
        artist_genre_sum_bpm,
        bpm_change,
        bpm_sum_6_prior,
        min_duration_sec_so_far,
        last_duration_sec,
        artist_genre_sum_duration_sec,
        bpm_vs_artist_genre_avg,
        bpm_rank,
        duration_sec_seq,
        SIZE(COLLECT_SET(artist_country) OVER w_artist_id) AS distinct_artist_country_in_artist_id,
        ROUND(artist_genre_sum_bpm - AVG(artist_genre_sum_bpm) OVER (PARTITION BY artist_id), 2) AS artist_genre_sum_bpm_vs_artist_id_avg,
        NTILE(4) OVER (ORDER BY bpm_rank ASC, id) AS bpm_rank_quartile,
        bpm_vs_artist_genre_avg - LAG(bpm_vs_artist_genre_avg) OVER (ORDER BY bpm_rank ASC, id) AS bpm_vs_artist_genre_avg_change,
        FIRST_VALUE(bpm_avg_3_prior) IGNORE NULLS OVER (ORDER BY bpm_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm_avg_3_prior,
        ROUND(CUME_DIST() OVER (ORDER BY bpm_rank ASC, id), 3) AS bpm_rank_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm_rank ASC), 3) AS bpm_rank_percent_rank,
        bpm_change - LAG(bpm_change) OVER (PARTITION BY duration_sec_seq ORDER BY artist_genre_sum_bpm ASC, id) AS bpm_change_change
    FROM metrics
    WINDOW w_artist_id AS (PARTITION BY artist_id)
)
SELECT
    id,
    artist_id,
    bpm,
    artist_name,
    artist_country,
    distinct_artist_id_in_artist_genre,
    bpm_cume_dist,
    artist_genre_sum_bpm,
    bpm_change,
    bpm_sum_6_prior,
    last_duration_sec,
    artist_genre_sum_duration_sec,
    bpm_rank,
    distinct_artist_country_in_artist_id,
    bpm_vs_artist_genre_avg_change,
    bpm_rank_cume_dist,
    bpm_rank_percent_rank,
    bpm_change_change
FROM metrics_2
WHERE bpm_vs_artist_genre_avg_change <= 108.0
ORDER BY artist_name, id
LIMIT 20
