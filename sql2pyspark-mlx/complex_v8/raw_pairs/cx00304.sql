WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE device = 'car'
      AND ms_played BETWEEN 72464 AND 257789
),
enriched_listener AS (
    -- Join listeners
    SELECT s.*, l.country AS listener_country
    FROM cleaned s
    JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        listener_country,
        ROUND(PERCENT_RANK() OVER (ORDER BY played_at), 3) AS played_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY device, listener_country ORDER BY played_at NULLS LAST), 3) AS played_at_cume_dist,
        DENSE_RANK() OVER w_all_by_played_at AS played_at_dense_rank,
        NTILE(5) OVER (PARTITION BY device, listener_country ORDER BY played_at NULLS LAST, id) AS played_at_quintile,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (ORDER BY played_at, id))) / 60, 1) AS minutes_since_prev_played_at,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at DESC, id))) / 60, 1) AS minutes_since_prev_played_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY played_at DESC, id), 3) AS played_at_percent_rank_2,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY played_at DESC NULLS FIRST) AS played_at_dense_rank_2,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY played_at DESC, id) AS played_at_dense_rank_3,
        NTILE(5) OVER w_track_id_by_played_at AS played_at_quintile_2,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY device, listener_country ORDER BY played_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at_3,
        NTILE(10) OVER w_listener_id_listener_country AS played_at_decile
    FROM enriched_listener
    WINDOW w_all_by_played_at AS (ORDER BY played_at, id), w_track_id_by_played_at AS (PARTITION BY track_id ORDER BY played_at DESC, id), w_listener_id_listener_country AS (PARTITION BY listener_id, listener_country ORDER BY played_at DESC, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(played_at_dense_rank_2 / 2) * 2 AS played_at_dense_rank_2_bucket,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM with_trends
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        device,
        listener_country,
        played_at_percent_rank,
        played_at_cume_dist,
        played_at_dense_rank,
        played_at_quintile,
        minutes_since_prev_played_at,
        minutes_since_prev_played_at_2,
        played_at_percent_rank_2,
        played_at_dense_rank_2,
        played_at_dense_rank_3,
        played_at_quintile_2,
        played_at_decile,
        device_prefix,
        MIN(played_at_quintile) OVER (PARTITION BY played_at_dense_rank_2_bucket, device) AS played_at_dense_rank_2_bucket_device_min_played_,
        MAX(played_at_decile) OVER (PARTITION BY played_at_dense_rank, listener_id ORDER BY played_at_percent_rank DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS played_at_decile_max_2_prior,
        ROUND(AVG(played_at_percent_rank) OVER (PARTITION BY played_at_dense_rank_2_bucket, device), 2) AS played_at_dense_rank_2_bucket_device_avg_played_,
        ROUND(played_at_dense_rank - AVG(played_at_dense_rank) OVER (PARTITION BY played_at_dense_rank_2_bucket, device), 2) AS played_at_dense_rank_vs_played_at_dense_rank_2_b,
        DENSE_RANK() OVER (PARTITION BY played_at_dense_rank, listener_id ORDER BY played_at_percent_rank DESC) AS played_at_percent_rank_dense_rank,
        ROUND(played_at_quintile_2 - AVG(played_at_quintile_2) OVER (PARTITION BY played_at_dense_rank_2_bucket, device), 2) AS played_at_quintile_2_vs_played_at_dense_rank_2_b
    FROM scored
)
-- Final projection
SELECT
    id,
    played_at,
    listener_country,
    played_at_cume_dist,
    played_at_dense_rank,
    played_at_percent_rank_2,
    played_at_dense_rank_2,
    played_at_dense_rank_3,
    played_at_quintile_2,
    played_at_decile,
    device_prefix,
    played_at_decile_max_2_prior,
    played_at_dense_rank_2_bucket_device_avg_played_,
    played_at_dense_rank_vs_played_at_dense_rank_2_b,
    played_at_percent_rank_dense_rank,
    played_at_quintile_2_vs_played_at_dense_rank_2_b
FROM sequenced
WHERE played_at_quintile_2 BETWEEN 3 AND 10
ORDER BY played_at_percent_rank_dense_rank NULLS LAST, played_at_decile_max_2_prior DESC
