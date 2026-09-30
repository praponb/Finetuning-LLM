WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        ARRAY_CONTAINS(moods, 'sad') AS has_sad,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'chill')
      AND is_explicit = TRUE
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.has_sad,
        s.duration_sec_bucket,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country
    FROM cleaned s
    JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        has_sad,
        duration_sec_bucket,
        artist_debut_year,
        artist_is_verified,
        artist_country,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_is_verified, artist_country)) AS distinct_artist_id_in_artist_is_verified_artist_,
        duration_sec_bucket - LAG(duration_sec_bucket) OVER w_has_sad_by_bpm AS duration_sec_bucket_change,
        DENSE_RANK() OVER (PARTITION BY has_sad ORDER BY bpm ASC) AS bpm_dense_rank,
        ROUND(CUME_DIST() OVER w_has_sad_by_bpm, 3) AS bpm_cume_dist,
        SUM(duration_sec_bucket) OVER (PARTITION BY has_sad ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_bucket,
        DENSE_RANK() OVER (PARTITION BY has_sad, artist_is_verified ORDER BY bpm, id) AS bpm_dense_rank_2,
        MAX(artist_debut_year) OVER (PARTITION BY artist_is_verified, artist_country) AS artist_is_verified_artist_country_max_artist_deb,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        RANK() OVER (PARTITION BY has_sad ORDER BY duration_sec) AS duration_sec_rank,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY artist_id)) AS distinct_artist_country_in_artist_id,
        SUM(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_is_verified, artist_country) AS rows_in_artist_is_verified_artist_country
    FROM with_details_artist
    WINDOW w_has_sad_by_bpm AS (PARTITION BY has_sad ORDER BY bpm ASC, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        has_sad,
        artist_country,
        ARRAY_SORT(COLLECT_SET(artist_country)) AS artist_country_list,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        SUM(artist_id_sum_duration_sec) FILTER (WHERE NOT artist_is_verified) AS artist_id_sum_duration_sec_not_artist_is_verifie,
        SUM(bpm_dense_rank_2) AS total_bpm_dense_rank_2,
        COUNT(*) AS cnt
    FROM sequenced
    GROUP BY artist_id, has_sad, artist_country
)
-- Final projection
SELECT
    artist_id,
    has_sad,
    artist_country_list,
    title_list,
    artist_id_sum_duration_sec_not_artist_is_verifie,
    cnt,
    FLOOR(total_bpm_dense_rank_2 / 10) * 10 AS total_bpm_dense_rank_2_bucket,
    IF(has_sad, 'yes', 'no') AS has_sad_label
FROM per_group
ORDER BY cnt
