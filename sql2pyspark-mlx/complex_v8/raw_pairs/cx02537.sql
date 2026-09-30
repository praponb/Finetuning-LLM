WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE duration_sec >= 144
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        bpm,
        moods,
        LAG(bpm_bucket) OVER w_artist_id_by_bpm AS prev_bpm_bucket,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        NTH_VALUE(bpm_bucket, 2) OVER (PARTITION BY artist_id ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_bucket,
        MAX(bpm_bucket) OVER (PARTITION BY artist_id) AS artist_id_max_bpm_bucket,
        duration_sec - LAG(duration_sec) OVER w_artist_id_by_bpm AS duration_sec_change,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY bpm ASC), 3) AS bpm_cume_dist,
        MAX(bpm) OVER (ORDER BY release_date ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS bpm_max_2_prior,
        MIN(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date NULLS LAST, id) AS rn,
        LAST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY duration_sec ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        LAG(bpm_bucket) OVER (ORDER BY duration_sec ASC NULLS LAST, id) AS prev_bpm_bucket_2
    FROM base
    WINDOW w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm ASC, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        bpm,
        moods,
        prev_bpm_bucket,
        second_bpm_bucket,
        artist_id_max_bpm_bucket,
        duration_sec_change,
        bpm_share_of_artist_id,
        bpm_cume_dist,
        artist_id_min_duration_sec,
        COALESCE(bpm_max_2_prior, 0) AS bpm_max_2_prior_filled
    FROM metrics
    WHERE bpm_max_2_prior < 420
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_bpm_share_of_artist_i, 3) AS bpm_share_of_artist_id_percent_rank,
        DENSE_RANK() OVER (ORDER BY bpm_max_2_prior_filled DESC, id) AS bpm_max_2_prior_filled_dense_rank,
        bpm_cume_dist - LAG(bpm_cume_dist) OVER (ORDER BY artist_id_min_duration_sec, id) AS bpm_cume_dist_change
    FROM enriched
    WINDOW w_all_by_bpm_share_of_artist_i AS (ORDER BY bpm_share_of_artist_id DESC, id)
)
SELECT
    id,
    bpm,
    moods,
    prev_bpm_bucket,
    second_bpm_bucket,
    artist_id_max_bpm_bucket,
    duration_sec_change,
    artist_id_min_duration_sec,
    bpm_max_2_prior_filled,
    bpm_share_of_artist_id_percent_rank,
    bpm_max_2_prior_filled_dense_rank,
    bpm_cume_dist_change,
    CASE WHEN bpm >= 142 THEN 'A' WHEN bpm >= 100 THEN 'B' ELSE 'C' END AS bpm_tier
FROM with_windows
ORDER BY bpm_tier DESC NULLS FIRST
