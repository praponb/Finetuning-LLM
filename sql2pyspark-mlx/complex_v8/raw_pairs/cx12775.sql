WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM tracks
    WHERE duration_sec >= 209
      AND YEAR(release_date) = 2025
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.release_date_quarter,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        a.artist_name
    FROM base_tracks s
    JOIN artists a ON a.id = s.artist_id AND a.is_verified = TRUE
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        release_date_quarter,
        artist_is_verified,
        artist_name,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY release_date_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        LAST_VALUE(release_date) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date) AS release_date_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY release_date, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date_quarter ASC NULLS LAST), 3) AS release_date_quarter_percent_rank,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY release_date_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY release_date, id), 3) AS release_date_percent_rank,
        DENSE_RANK() OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY release_date, id) AS release_date_dense_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY release_date), 3) AS release_date_cume_dist
    FROM combined_details_artist
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(release_date_dense_rank - AVG(release_date_dense_rank) OVER w_artist_is_verified_release_d, 2) AS release_date_dense_rank_vs_artist_is_verified_re,
        LAG(release_date, 3) OVER (PARTITION BY release_date_dense_rank_2 ORDER BY last_known_release_date, id) AS prev3_release_date,
        ROUND(release_date_dense_rank_2 - AVG(release_date_dense_rank_2) OVER (PARTITION BY artist_is_verified, release_date_dense_rank_2), 2) AS release_date_dense_rank_2_vs_artist_is_verified_,
        FIRST_VALUE(release_date_quarter) OVER (PARTITION BY release_date_dense_rank ORDER BY last_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date_quarter,
        COUNT(*) OVER (PARTITION BY artist_is_verified, release_date_dense_rank_2) AS rows_in_artist_is_verified_release_date_dense_ra
    FROM metrics
    WINDOW w_artist_is_verified_release_d AS (PARTITION BY artist_is_verified, release_date_dense_rank_2)
)
SELECT
    id,
    artist_id,
    release_date,
    is_explicit,
    artist_is_verified,
    artist_name,
    last_known_release_date,
    last_release_date,
    release_date_dense_rank,
    second_release_date,
    first_known_release_date,
    release_date_dense_rank_2,
    release_date_cume_dist,
    release_date_dense_rank_vs_artist_is_verified_re,
    prev3_release_date,
    release_date_dense_rank_2_vs_artist_is_verified_,
    first_release_date_quarter
FROM windowed
WHERE first_release_date_quarter >= TIMESTAMP '2025-01-21 09:00:00'
ORDER BY artist_id DESC
