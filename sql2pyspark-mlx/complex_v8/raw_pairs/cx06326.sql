WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        bpm - duration_sec AS bpm_minus_duration_sec
    FROM tracks
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.duration_sec_bucket,
        s.bpm_minus_duration_sec,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM base_tracks s
    JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        artist_debut_year - LAG(artist_debut_year) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC, id) AS artist_debut_year_change,
        NTILE(5) OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY bpm_minus_duration_sec DESC, id) AS bpm_minus_duration_sec_quintile,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY is_explicit, artist_id)) AS distinct_artist_genre_in_is_explicit_artist_id,
        RANK() OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY bpm_minus_duration_sec DESC, id) AS bpm_minus_duration_sec_rank,
        ROUND(PERCENT_RANK() OVER w_artist_is_verified_is_explic_2, 3) AS bpm_minus_duration_sec_percent_rank,
        SUM(bpm_minus_duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_bpm_minus_duration_sec,
        MIN(duration_sec_bucket) OVER (PARTITION BY artist_genre ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_bucket_so_far,
        ROUND(duration_sec_bucket / NULLIF(SUM(duration_sec_bucket) OVER (PARTITION BY is_explicit, artist_id), 0), 4) AS duration_sec_bucket_share_of_is_explicit_artist_,
        ROW_NUMBER() OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY bpm_minus_duration_sec DESC, id) AS rn
    FROM enriched
    WINDOW w_artist_is_verified_is_explic_2 AS (PARTITION BY artist_is_verified, is_explicit ORDER BY bpm_minus_duration_sec DESC)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE bpm_minus_duration_sec_rank <= 2
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(duration_sec) OVER w_artist_genre_rn, 2) AS artist_genre_rn_avg_duration_sec,
        FIRST_VALUE(duration_sec_bucket) IGNORE NULLS OVER (PARTITION BY artist_genre, rn ORDER BY distinct_artist_genre_in_is_explicit_artist_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_bucket,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_genre, rn)) AS distinct_artist_id_in_artist_genre_rn,
        FIRST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY rn, bpm_minus_duration_sec_quintile ORDER BY min_duration_sec_bucket_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_artist_debut_year
    FROM leaders
    WINDOW w_artist_genre_rn AS (PARTITION BY artist_genre, rn)
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    duration_sec_bucket,
    bpm_minus_duration_sec,
    bpm_minus_duration_sec_quintile,
    distinct_artist_genre_in_is_explicit_artist_id,
    bpm_minus_duration_sec_rank,
    bpm_minus_duration_sec_percent_rank,
    artist_id_sum_bpm_minus_duration_sec,
    duration_sec_bucket_share_of_is_explicit_artist_,
    rn,
    artist_genre_rn_avg_duration_sec,
    first_known_duration_sec_bucket,
    FLOOR(bpm_minus_duration_sec / 100) * 100 AS bpm_minus_duration_sec_bucket
FROM metrics
ORDER BY bpm_minus_duration_sec_quintile DESC NULLS FIRST, rn DESC NULLS FIRST
