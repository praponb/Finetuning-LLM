WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE bpm <= 127
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM cleaned s
    LEFT JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        artist_is_verified,
        artist_debut_year,
        SUM(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_6_prior,
        ROUND(CUME_DIST() OVER w_artist_id_artist_is_verified, 3) AS artist_debut_year_cume_dist,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_7,
        artist_debut_year - LAG(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS artist_debut_year_change,
        NTILE(5) OVER w_artist_is_verified_is_explic AS duration_sec_quintile,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS bpm_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY duration_sec DESC), 3) AS duration_sec_percent_rank,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2,
        LAG(duration_sec, 2) OVER (ORDER BY duration_sec, id) AS prev2_duration_sec,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC) AS duration_sec_dense_rank,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_id, artist_is_verified ORDER BY artist_debut_year NULLS LAST, id) AS duration_sec_change,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_is_verified_is_explic_3, 0), 4) AS duration_sec_share_of_artist_is_verified_is_expl,
        SUM(duration_sec) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY artist_debut_year, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_3_prior
    FROM combined_details_artist
    WINDOW w_artist_id_artist_is_verified AS (PARTITION BY artist_id, artist_is_verified ORDER BY artist_debut_year NULLS LAST), w_artist_is_verified_is_explic AS (PARTITION BY artist_is_verified, is_explicit ORDER BY duration_sec DESC, id), w_artist_is_verified_is_explic_3 AS (PARTITION BY artist_is_verified, is_explicit)
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    is_explicit,
    artist_is_verified,
    artist_debut_year_cume_dist,
    duration_sec_avg_7,
    duration_sec_quintile,
    duration_sec_percent_rank,
    duration_sec_avg_2,
    prev2_duration_sec,
    duration_sec_share_of_artist_is_verified_is_expl,
    duration_sec_sum_3_prior,
    FLOOR(duration_sec_avg_7 / 50) * 50 AS duration_sec_avg_7_bucket
FROM metrics
ORDER BY title
