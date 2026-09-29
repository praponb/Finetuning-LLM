WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM plays
    WHERE track_id <= 13
      AND played_at < TIMESTAMP '2025-02-18 12:00:00'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source, listener_id), 2) AS ms_played_vs_source_listener_id_avg,
        NTILE(10) OVER w_all_by_ms_played AS ms_played_decile,
        LAG(played_at, 2) OVER (PARTITION BY track_id ORDER BY played_at_year, id) AS prev2_played_at,
        ROW_NUMBER() OVER (ORDER BY ms_played, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY source ORDER BY played_at_year DESC, id), 3) AS played_at_year_cume_dist,
        ROUND(ms_played_bucket / NULLIF(SUM(ms_played_bucket) OVER w_source_listener_id, 0), 4) AS ms_played_bucket_share_of_source_listener_id,
        LAST_VALUE(played_at_year) OVER (PARTITION BY track_id ORDER BY played_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_year,
        ROW_NUMBER() OVER w_track_id_by_played_at_year AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source ORDER BY played_at_year DESC), 3) AS played_at_year_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY source ORDER BY played_at_year DESC, id) AS rn_2,
        ROUND(AVG(ms_played) OVER (PARTITION BY source, listener_id), 2) AS source_listener_id_avg_ms_played,
        LEAD(ms_played_bucket, 3) OVER (ORDER BY ms_played, id) AS next3_ms_played_bucket,
        ROUND(AVG(ms_played_bucket) OVER (ORDER BY ms_played, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_bucket_avg_2,
        MIN(ms_played) OVER (PARTITION BY source, listener_id) AS source_listener_id_min_ms_played
    FROM cleaned
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played, id), w_source_listener_id AS (PARTITION BY source, listener_id), w_track_id_by_played_at_year AS (PARTITION BY track_id ORDER BY played_at_year, id)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        played_at,
        device,
        ms_played_bucket,
        ms_played_vs_source_listener_id_avg,
        prev2_played_at,
        rn,
        played_at_year_cume_dist,
        last_played_at_year,
        seq_no,
        played_at_year_percent_rank,
        rn_2,
        source_listener_id_avg_ms_played,
        ms_played_bucket_avg_2,
        source_listener_id_min_ms_played,
        DATE_FORMAT(last_played_at_year, 'yyyy-MM-dd') AS last_played_at_year_label,
        ROUND(played_at_year_cume_dist / NULLIF(ms_played_bucket, 0), 3) AS played_at_year_cume_dist_per_ms_played_bucket,
        next3_ms_played_bucket - played_at_year_cume_dist AS next3_ms_played_bucket_minus_played_at_year_cume
    FROM metrics
),
stats AS (
    -- Group-level totals
    SELECT
        rn,
        seq_no,
        device,
        MAX(source_listener_id_avg_ms_played) AS max_source_listener_id_avg_ms_played,
        ROUND(SUM(rn) / NULLIF(SUM(played_at_year_percent_rank), 0), 3) AS rn_to_played_at_year_percent_rank_ratio,
        COUNT(*) AS cnt
    FROM scored
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
)
SELECT
    rn,
    device,
    max_source_listener_id_avg_ms_played,
    rn_to_played_at_year_percent_rank_ratio,
    cnt,
    COALESCE(rn, 0) AS rn_filled
FROM stats
WHERE rn <= 9
ORDER BY max_source_listener_id_avg_ms_played
