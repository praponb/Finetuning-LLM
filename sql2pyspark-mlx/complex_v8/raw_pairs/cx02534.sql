WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm
    FROM tracks
    WHERE bpm >= 104
      AND LENGTH(TRIM(title)) >= 4
),
enriched AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        a.genre AS artist_genre,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM filtered s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        DENSE_RANK() OVER w_artist_genre_by_duration_sec AS duration_sec_dense_rank,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY artist_id)) AS distinct_artist_country_in_artist_id,
        ROW_NUMBER() OVER (PARTITION BY artist_id, artist_country ORDER BY duration_sec DESC, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_artist_id_artist_country_by, 3) AS duration_sec_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_genre ORDER BY duration_sec NULLS LAST, id), 3) AS duration_sec_percent_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_is_verified ORDER BY duration_sec), 3) AS duration_sec_percent_rank_3,
        SUM(duration_sec) OVER (PARTITION BY artist_id, artist_country ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_3_prior,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY artist_id)) AS distinct_artist_genre_in_artist_id
    FROM enriched
    WINDOW w_artist_genre_by_duration_sec AS (PARTITION BY artist_genre ORDER BY duration_sec NULLS LAST), w_artist_id_artist_country_by AS (PARTITION BY artist_id, artist_country ORDER BY duration_sec DESC, id)
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    bpm,
    artist_genre,
    artist_is_verified,
    bpm_vs_artist_id_avg,
    duration_sec_dense_rank,
    distinct_artist_country_in_artist_id,
    duration_sec_percent_rank,
    duration_sec_percent_rank_2,
    duration_sec_percent_rank_3,
    duration_sec_sum_3_prior,
    artist_id_avg_duration_sec,
    rows_in_artist_id,
    distinct_artist_genre_in_artist_id,
    CASE WHEN artist_is_verified THEN 1 ELSE 0 END AS artist_is_verified_flag,
    GREATEST(duration_sec_dense_rank, duration_sec) AS greatest_duration_sec_dense_rank_duration_sec
FROM windowed
ORDER BY title DESC
