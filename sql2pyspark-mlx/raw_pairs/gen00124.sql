WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        firmware,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY pressure, id) AS pressure_bucket,
        ROW_NUMBER() OVER (ORDER BY temp_c, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS seq_no
    FROM device_readings
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        device_code,
        status_bits,
        firmware,
        seq_no
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY seq_no NULLS FIRST) AS latest_rank
        FROM metrics
    ) t
    WHERE latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_bits) OVER (ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_bits_avg_6_prior,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_bits,
        AVG(status_bits) OVER (ORDER BY status_bits, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS status_bits_avg_5
    FROM latest
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        firmware,
        seq_no,
        cumulative_status_bits,
        status_bits_avg_5,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY status_bits DESC) AS status_bits_dense_rank,
        AVG(cumulative_status_bits) OVER (PARTITION BY device_code) AS device_code_avg_cumulative_status_bits,
        LAST_VALUE(seq_no) IGNORE NULLS OVER w_hist AS last_known_seq_no
    FROM scored
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY cumulative_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        firmware,
        cumulative_status_bits,
        status_bits_dense_rank,
        device_code_avg_cumulative_status_bits,
        FLOOR(device_code_avg_cumulative_status_bits) AS device_code_avg_cumulative_status_bits_whole
    FROM enriched
    WHERE status_bits_dense_rank != 3
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        firmware,
        status_bits_dense_rank,
        NTILE(10) OVER (ORDER BY status_bits_dense_rank DESC, id) AS status_bits_dense_rank_bucket,
        LAG(status_bits_dense_rank, 1) OVER (ORDER BY status_bits_dense_rank, id) AS prev_status_bits_dense_rank
    FROM filtered
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        firmware,
        status_bits_dense_rank_bucket,
        prev_status_bits_dense_rank,
        LAG(status_bits_dense_rank_bucket) OVER w_running AS prev_status_bits_dense_rank_bucket,
        LEAD(status_bits_dense_rank_bucket) OVER w_running AS next_status_bits_dense_rank_bucket,
        LEAD(prev_status_bits_dense_rank) OVER w_running AS next_prev_status_bits_dense_rank,
        LAG(prev_status_bits_dense_rank) OVER w_running AS prev_prev_status_bits_dense_rank,
        ROW_NUMBER() OVER w_running AS event_seq,
        NTILE(4) OVER (ORDER BY status_bits_dense_rank_bucket DESC, id) AS status_bits_dense_rank_bucket_bucket
    FROM metrics_device_readings
    WHERE prev_status_bits_dense_rank BETWEEN 3 AND 6
    WINDOW w_running AS (ORDER BY status_bits_dense_rank_bucket, id)
)
-- Grouped metrics
SELECT
    CASE WHEN prev_status_bits_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_status_bits_dense_rank_tier,
    CASE WHEN prev_prev_status_bits_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS prev_prev_status_bits_dense_rank_tier,
    ROUND(AVG(next_status_bits_dense_rank_bucket), 2) AS avg_next_status_bits_dense_rank_bucket,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(prev_status_bits_dense_rank_bucket, 0.95) AS prev_status_bits_dense_rank_bucket_p95
FROM with_windows
GROUP BY CASE WHEN prev_status_bits_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_prev_status_bits_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(DISTINCT id) > 1
ORDER BY prev_status_bits_dense_rank_bucket_p95 NULLS LAST;
