WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'EST'
      AND site_id >= 6
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(voltage) OVER w_all_by_kwh AS next_voltage,
        ROW_NUMBER() OVER (ORDER BY kwh DESC, id) AS kwh_seq,
        voltage - LAG(voltage) OVER (PARTITION BY site_id ORDER BY kwh, id) AS voltage_change,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, quality), 2) AS site_id_quality_avg_kwh,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality), 0), 4) AS voltage_share_of_quality,
        ROUND(voltage - AVG(voltage) OVER w_site_id_quality, 2) AS voltage_vs_site_id_quality_avg,
        ROUND(PERCENT_RANK() OVER w_site_id_quality_by_voltage, 3) AS voltage_percent_rank,
        DENSE_RANK() OVER w_all_by_kwh AS kwh_dense_rank,
        DENSE_RANK() OVER (ORDER BY kwh DESC) AS kwh_dense_rank_2
    FROM recent_readings
    WINDOW w_all_by_kwh AS (ORDER BY kwh DESC, id), w_site_id_quality AS (PARTITION BY site_id, quality), w_site_id_quality_by_voltage AS (PARTITION BY site_id, quality ORDER BY voltage DESC, id)
)
-- Final projection
SELECT
    id,
    kwh,
    quality,
    next_voltage,
    kwh_seq,
    voltage_change,
    site_id_quality_avg_kwh,
    voltage_share_of_quality,
    voltage_vs_site_id_quality_avg,
    kwh_dense_rank,
    kwh_dense_rank_2,
    CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group,
    kwh_seq + kwh AS kwh_seq_plus_kwh
FROM sequenced
ORDER BY kwh DESC, voltage_change, id
LIMIT 5
