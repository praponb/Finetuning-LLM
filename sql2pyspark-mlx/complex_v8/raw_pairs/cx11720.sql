WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER w_quality_site_id, 2) AS voltage_vs_quality_site_id_avg,
        MAX(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_max_kwh,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY read_at), 3) AS read_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(read_at) ORDER BY read_at) AS read_at_dense_rank,
        COUNT(*) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, site_id), 0), 4) AS kwh_share_of_quality_site_id,
        NTILE(3) OVER w_read_at_year_by_read_at AS read_at_tercile,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        FIRST_VALUE(least_voltage_kwh) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_least_voltage_kwh,
        ROW_NUMBER() OVER (ORDER BY voltage DESC, id) AS rn,
        RANK() OVER (PARTITION BY site_id ORDER BY read_at, id) AS read_at_rank,
        SUM(voltage) OVER (PARTITION BY YEAR(read_at) ORDER BY read_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_sum_3,
        LAST_VALUE(least_voltage_kwh) IGNORE NULLS OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_least_voltage_kwh,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id
    FROM recent_readings
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_read_at_year_by_read_at AS (PARTITION BY YEAR(read_at) ORDER BY read_at, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_site_id_quality AS rows_in_site_id_quality,
        MAX(rows_in_quality_site_id) OVER (PARTITION BY read_at_tercile) AS read_at_tercile_max_rows_in_quality_site_id,
        NTH_VALUE(quality_site_id_max_kwh, 2) OVER (PARTITION BY quality ORDER BY read_at_tercile DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_quality_site_id_max_kwh,
        LEAD(read_at, 2) OVER (PARTITION BY quality ORDER BY read_at_tercile DESC NULLS FIRST, id) AS next2_read_at,
        read_at_percent_rank - LAG(read_at_percent_rank) OVER (PARTITION BY read_at_rank ORDER BY running_count, id) AS read_at_percent_rank_change,
        ROUND(AVG(read_at_rank) OVER (PARTITION BY read_at_rank ORDER BY running_count, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS read_at_rank_avg_2_prior
    FROM sequenced
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    quality,
    voltage_bucket,
    least_voltage_kwh,
    quality_site_id_max_kwh,
    read_at_dense_rank,
    running_count,
    read_at_tercile,
    second_voltage,
    first_least_voltage_kwh,
    rn,
    read_at_rank,
    voltage_sum_3,
    last_known_least_voltage_kwh,
    rows_in_site_id_quality,
    read_at_tercile_max_rows_in_quality_site_id,
    next2_read_at,
    read_at_percent_rank_change,
    read_at_rank_avg_2_prior,
    FLOOR(kwh / 25) * 25 AS kwh_bucket
FROM with_trends
WHERE voltage_bucket BETWEEN 222 AND 238
ORDER BY kwh, read_at_tercile NULLS LAST
