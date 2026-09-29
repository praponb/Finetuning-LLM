WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        INITCAP(TRIM(quality)) AS quality_clean,
        ABS(kwh - 86.0) AS kwh_distance
    FROM meter_readings
    WHERE site_id <= 6
      AND voltage < 241.9
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        kwh,
        voltage,
        quality,
        quality_ref,
        quality_clean,
        kwh_distance,
        COUNT(*) OVER w_site_id_quality AS rows_in_site_id_quality,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY read_at DESC, id), 3) AS read_at_cume_dist,
        NTH_VALUE(kwh, 2) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        COUNT(*) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER w_quality_by_read_at_ties, 3) AS read_at_percent_rank,
        RANK() OVER (PARTITION BY quality ORDER BY read_at DESC, id) AS read_at_rank
    FROM base
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality), w_quality_by_read_at_ties AS (PARTITION BY quality ORDER BY read_at DESC)
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE rows_in_site_id_quality < (SELECT MAX(rows_in_site_id_quality) FROM metrics)
)
-- Final projection
SELECT
    id,
    kwh,
    voltage,
    quality_clean,
    kwh_distance,
    rows_in_site_id_quality,
    read_at_cume_dist,
    second_kwh,
    running_count,
    read_at_percent_rank,
    read_at_rank,
    SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
    LENGTH(TRIM(quality_clean)) AS quality_clean_len
FROM screened
ORDER BY read_at_cume_dist DESC
