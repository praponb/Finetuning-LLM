WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage + kwh AS voltage_plus_kwh,
        kwh + voltage AS kwh_plus_voltage,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh
    FROM meter_readings
    WHERE kwh >= 16.5
      AND site_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        kwh,
        quality,
        voltage_plus_kwh,
        kwh_plus_voltage,
        greatest_voltage_kwh,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY read_at DESC) AS read_at_rank,
        ROUND(AVG(greatest_voltage_kwh) OVER (PARTITION BY quality ORDER BY voltage_plus_kwh, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS greatest_voltage_kwh_avg_2,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY kwh_plus_voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        ROUND(voltage_plus_kwh / NULLIF(SUM(voltage_plus_kwh) OVER (PARTITION BY quality), 0), 4) AS voltage_plus_kwh_share_of_quality,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh_plus_voltage DESC, id) AS kwh_plus_voltage_seq
    FROM filtered_readings
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE kwh > (SELECT MIN(kwh) FROM metrics)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        quality,
        read_at_rank,
        greatest_voltage_kwh_avg_2,
        distinct_site_id_in_quality,
        voltage_plus_kwh_share_of_quality,
        kwh_plus_voltage_seq,
        LAST_VALUE(read_at_rank) IGNORE NULLS OVER (PARTITION BY kwh_plus_voltage_seq, distinct_site_id_in_quality ORDER BY voltage_plus_kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at_rank,
        ROUND(AVG(voltage_plus_kwh) OVER (PARTITION BY distinct_site_id_in_quality, read_at_rank ORDER BY kwh_plus_voltage_seq, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS voltage_plus_kwh_avg_6_prior
    FROM filtered
)
-- Final projection
SELECT
    id,
    read_at_rank,
    greatest_voltage_kwh_avg_2,
    voltage_plus_kwh_share_of_quality,
    last_known_read_at_rank,
    voltage_plus_kwh_avg_6_prior,
    CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
FROM sequenced
ORDER BY read_at_rank
