WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        voltage,
        quality,
        MAX(kwh) OVER (ORDER BY kwh NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_max_4,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_3_prior,
        FIRST_VALUE(read_at) OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY kwh DESC), 3) AS kwh_percent_rank,
        ROUND(voltage - AVG(voltage) OVER w_quality_site_id, 2) AS voltage_vs_quality_site_id_avg,
        MAX(voltage) OVER (PARTITION BY site_id) AS site_id_max_voltage,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_2,
        SUM(kwh) OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh
    FROM filtered_readings
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id)
)
-- Final projection
SELECT
    id,
    voltage,
    kwh_max_4,
    first_read_at,
    site_id_max_voltage,
    second_voltage_2,
    running_kwh,
    SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
FROM sequenced
ORDER BY first_read_at, site_id_max_voltage DESC
