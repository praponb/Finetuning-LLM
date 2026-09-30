WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
),
combined_details AS (
    -- Join artists
    SELECT s.*, a.artist_name, a.is_verified AS artist_is_verified
    FROM base s
    LEFT JOIN artists a ON a.id = s.artist_id AND NOT a.is_verified
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY bpm, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_3,
        NTILE(3) OVER (ORDER BY bpm_bucket, id) AS bpm_bucket_tercile,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        MIN(bpm) OVER w_artist_id AS artist_id_min_bpm,
        SUM(bpm_bucket) OVER w_artist_id AS artist_id_sum_bpm_bucket,
        LAST_VALUE(bpm_bucket) OVER (ORDER BY bpm_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm_bucket,
        NTILE(3) OVER (ORDER BY bpm_bucket DESC, id) AS bpm_bucket_tercile_2,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        NTILE(5) OVER w_all_by_bpm_bucket AS bpm_bucket_quintile,
        ROW_NUMBER() OVER w_artist_id_artist_is_verified AS rn,
        NTH_VALUE(bpm_bucket, 2) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_bucket
    FROM combined_details
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_all_by_bpm_bucket AS (ORDER BY bpm_bucket, id), w_artist_id_artist_is_verified AS (PARTITION BY artist_id, artist_is_verified ORDER BY bpm DESC, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY artist_id_min_bpm DESC, id) AS artist_id_min_bpm_rank,
        MAX(rows_in_artist_id) OVER (PARTITION BY rn) AS rn_max_rows_in_artist_id,
        COUNT(*) OVER (PARTITION BY rn) AS rows_in_rn,
        SUM(bpm_bucket_quintile) OVER (PARTITION BY rn) AS rn_sum_bpm_bucket_quintile,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY rn)) AS distinct_artist_id_in_rn,
        MIN(bpm_bucket_tercile) OVER w_rn AS rn_min_bpm_bucket_tercile,
        FIRST_VALUE(bpm_bucket) IGNORE NULLS OVER (PARTITION BY rn, bpm_bucket_tercile_2 ORDER BY bpm_share_of_artist_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm_bucket,
        ROUND(bpm_bucket_tercile - AVG(bpm_bucket_tercile) OVER w_rn, 2) AS bpm_bucket_tercile_vs_rn_avg
    FROM ranked
    WINDOW w_rn AS (PARTITION BY rn)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        CASE WHEN rn_max_rows_in_artist_id >= 170 THEN 'A' WHEN rn_max_rows_in_artist_id >= 71 THEN 'B' ELSE 'C' END AS rn_max_rows_in_artist_id_tier,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids
    FROM metrics
    WHERE rows_in_rn > 5
    GROUP BY 1, 2
)
SELECT
    artist_id,
    rn_max_rows_in_artist_id_tier,
    distinct_artist_ids,
    n_rows,
    distinct_ids,
    distinct_ids + distinct_artist_ids AS distinct_ids_plus_distinct_artist_ids,
    COALESCE(distinct_ids, 0) AS distinct_ids_filled
FROM grouped
WHERE rn_max_rows_in_artist_id_tier IS NOT NULL
ORDER BY artist_id DESC NULLS FIRST, n_rows DESC
