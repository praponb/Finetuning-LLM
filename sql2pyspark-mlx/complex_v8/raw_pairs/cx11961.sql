WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE kwh > 20.4
      AND site_id >= 6
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        voltage,
        kwh_bucket,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh_bucket DESC NULLS FIRST), 3) AS kwh_bucket_percent_rank,
        NTH_VALUE(kwh, 2) OVER (ORDER BY kwh_bucket DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        MIN(kwh) OVER w_quality AS quality_min_kwh,
        MAX(kwh_bucket) OVER (ORDER BY kwh_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_bucket_so_far,
        SUM(voltage) OVER (PARTITION BY quality) AS quality_sum_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_quality, 0), 4) AS voltage_share_of_quality,
        ROUND(AVG(kwh) OVER (PARTITION BY quality ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_1_prior,
        FIRST_VALUE(kwh_bucket) IGNORE NULLS OVER (ORDER BY kwh_bucket DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh_bucket,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY voltage) AS voltage_dense_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id, 0), 4) AS voltage_share_of_site_id,
        LAST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY kwh_bucket DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh_2,
        ROW_NUMBER() OVER w_quality_by_voltage AS rn,
        MAX(kwh_bucket) OVER w_site_id AS site_id_max_kwh_bucket,
        ROUND(CUME_DIST() OVER (ORDER BY kwh_bucket DESC), 3) AS kwh_bucket_cume_dist
    FROM scoped_readings
    WINDOW w_quality AS (PARTITION BY quality), w_site_id AS (PARTITION BY site_id), w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage, id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        voltage,
        kwh_bucket,
        kwh_bucket_percent_rank,
        second_kwh,
        quality_min_kwh,
        max_kwh_bucket_so_far,
        quality_sum_voltage,
        voltage_share_of_quality,
        kwh_avg_1_prior,
        first_known_kwh_bucket,
        voltage_dense_rank,
        voltage_share_of_site_id,
        last_known_kwh,
        last_known_kwh_2,
        site_id_max_kwh_bucket,
        kwh_bucket_cume_dist
    FROM metrics
    WHERE rn = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        voltage,
        kwh_bucket,
        kwh_bucket_percent_rank,
        second_kwh,
        max_kwh_bucket_so_far,
        first_known_kwh_bucket,
        voltage_dense_rank,
        voltage_share_of_site_id,
        last_known_kwh,
        last_known_kwh_2,
        site_id_max_kwh_bucket,
        kwh_bucket_cume_dist,
        ROUND(AVG(second_kwh) OVER (PARTITION BY voltage_dense_rank ORDER BY second_kwh DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS second_kwh_avg_3_prior,
        SUM(kwh_bucket_cume_dist) OVER w_voltage_dense_rank AS voltage_dense_rank_sum_kwh_bucket_cume_dist,
        SUM(voltage_dense_rank) OVER (PARTITION BY voltage_dense_rank ORDER BY kwh_bucket DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_dense_rank_sum_3,
        FIRST_VALUE(quality_sum_voltage) OVER (PARTITION BY voltage_dense_rank ORDER BY kwh_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_quality_sum_voltage,
        LAG(first_known_kwh_bucket, 1, 0) OVER (PARTITION BY voltage_dense_rank ORDER BY second_kwh ASC NULLS LAST, id) AS prev_first_known_kwh_bucket,
        ROUND(AVG(kwh_avg_1_prior) OVER (PARTITION BY voltage_dense_rank), 2) AS voltage_dense_rank_avg_kwh_avg_1_prior,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY voltage_dense_rank ORDER BY site_id_max_kwh_bucket ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage
    FROM first_rows
    WINDOW w_voltage_dense_rank AS (PARTITION BY voltage_dense_rank)
)
SELECT
    id,
    kwh_bucket_percent_rank,
    second_kwh,
    first_known_kwh_bucket,
    voltage_dense_rank,
    voltage_share_of_site_id,
    last_known_kwh,
    last_known_kwh_2,
    site_id_max_kwh_bucket,
    second_kwh_avg_3_prior,
    voltage_dense_rank_sum_kwh_bucket_cume_dist,
    voltage_dense_rank_sum_3,
    first_quality_sum_voltage,
    prev_first_known_kwh_bucket,
    voltage_dense_rank_avg_kwh_avg_1_prior,
    first_known_voltage
FROM ranked
WHERE kwh_bucket >= 42
ORDER BY last_known_kwh DESC, voltage_dense_rank DESC
