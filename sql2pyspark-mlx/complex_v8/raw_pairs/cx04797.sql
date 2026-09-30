WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 3)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LEAD(kwh, 1, 0) OVER w_site_id_by_voltage AS next_kwh,
        SUM(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_sum_4,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_id), 2) AS kwh_vs_quality_site_id_avg,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY voltage, id), 3) AS voltage_cume_dist,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        ROW_NUMBER() OVER w_site_id_by_voltage AS rn,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_quality_site_id, 0), 4) AS voltage_share_of_quality_site_id,
        RANK() OVER (ORDER BY voltage DESC) AS voltage_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY voltage), 3) AS voltage_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY voltage DESC, id), 3) AS voltage_percent_rank_2,
        NTILE(10) OVER w_quality_by_voltage AS voltage_decile,
        MIN(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_kwh,
        voltage - LAG(voltage) OVER (PARTITION BY quality ORDER BY voltage, id) AS voltage_change,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage
    FROM base_readings
    WINDOW w_site_id_by_voltage AS (PARTITION BY site_id ORDER BY voltage, id), w_quality_site_id AS (PARTITION BY quality, site_id), w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(voltage_cume_dist) OVER w_rn, 2) AS rn_avg_voltage_cume_dist,
        FIRST_VALUE(next_kwh) OVER (ORDER BY quality_site_id_min_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next_kwh,
        RANK() OVER (ORDER BY quality_site_id_min_kwh) AS quality_site_id_min_kwh_rank,
        ROUND(kwh - AVG(kwh) OVER w_rn, 2) AS kwh_vs_rn_avg
    FROM metrics
    WINDOW w_rn AS (PARTITION BY rn)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    next_kwh,
    voltage_sum_4,
    voltage_avg_7,
    voltage_cume_dist,
    rn,
    voltage_share_of_quality_site_id,
    voltage_percent_rank_2,
    voltage_decile,
    quality_site_id_min_kwh,
    voltage_change,
    first_known_voltage,
    rn_avg_voltage_cume_dist,
    ROUND(quality_site_id_min_kwh / NULLIF(rn, 0), 3) AS quality_site_id_min_kwh_per_rn,
    CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
FROM windowed
ORDER BY site_id
