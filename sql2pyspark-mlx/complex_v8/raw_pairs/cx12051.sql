WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        SUM(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_sum_kwh,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality), 2) AS voltage_vs_quality_avg,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_id), 2) AS kwh_vs_quality_site_id_avg,
        ROW_NUMBER() OVER (ORDER BY kwh ASC, id) AS rn,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality), 0), 4) AS kwh_share_of_quality,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        LAST_VALUE(voltage) OVER (PARTITION BY quality ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage
    FROM valid_readings
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    last_known_kwh,
    quality_site_id_sum_kwh,
    voltage_vs_site_id_avg,
    voltage_vs_quality_avg,
    minutes_since_prev_read_at,
    kwh_vs_quality_site_id_avg,
    rn,
    kwh_share_of_quality,
    rows_in_quality_site_id
FROM sequenced
WHERE voltage <= 241.8
ORDER BY voltage_vs_quality_avg
