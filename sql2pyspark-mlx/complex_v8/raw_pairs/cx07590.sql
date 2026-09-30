WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        UPPER(TRIM(quality)) AS quality_clean,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE (voltage < 241.9 OR site_id IS NOT NULL)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        kwh,
        quality,
        quality_clean,
        RANK() OVER w_all_by_read_at AS read_at_rank,
        COUNT(*) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at)) AS rows_in_site_id_read_at_month,
        DENSE_RANK() OVER w_all_by_read_at_ties AS read_at_dense_rank,
        MAX(kwh_bucket) OVER (ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_bucket_max_2_prior,
        COUNT(*) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(kwh_bucket) OVER (ORDER BY read_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_bucket_sum_1_prior
    FROM cleaned
    WINDOW w_all_by_read_at AS (ORDER BY read_at DESC, id), w_all_by_read_at_ties AS (ORDER BY read_at DESC)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        kwh,
        quality_clean,
        read_at_rank,
        rows_in_site_id_read_at_month,
        running_count,
        kwh_bucket_sum_1_prior,
        LENGTH(TRIM(quality)) AS quality_len,
        ROUND(kwh_bucket_max_2_prior / NULLIF(kwh, 0), 3) AS kwh_bucket_max_2_prior_per_kwh
    FROM ranked
)
-- Final projection
SELECT
    id,
    quality_clean,
    read_at_rank,
    rows_in_site_id_read_at_month,
    running_count,
    kwh_bucket_sum_1_prior,
    quality_len,
    kwh_bucket_max_2_prior_per_kwh,
    LENGTH(TRIM(quality_clean)) AS quality_clean_len
FROM derived
ORDER BY read_at_rank
