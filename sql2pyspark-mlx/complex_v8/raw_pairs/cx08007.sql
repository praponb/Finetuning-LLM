WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id >= 2
      AND read_at < TIMESTAMP '2025-02-24 10:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        ROUND(voltage - AVG(voltage) OVER w_quality, 2) AS voltage_vs_quality_avg,
        LAST_VALUE(kwh) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        MIN(voltage) OVER w_site_id AS site_id_min_voltage,
        LEAD(voltage, 1, 0) OVER w_quality_by_read_at AS next_voltage,
        MIN(kwh) OVER (PARTITION BY quality) AS quality_min_kwh,
        ROUND(PERCENT_RANK() OVER w_site_id_quality_by_voltage, 3) AS voltage_percent_rank,
        FIRST_VALUE(voltage) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        ROUND(AVG(voltage) OVER (PARTITION BY quality), 2) AS quality_avg_voltage,
        SUM(kwh) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_sum_3
    FROM base
    WINDOW w_quality AS (PARTITION BY quality), w_site_id AS (PARTITION BY site_id), w_quality_by_read_at AS (PARTITION BY quality ORDER BY read_at DESC, id), w_site_id_quality_by_voltage AS (PARTITION BY site_id, quality ORDER BY voltage DESC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    rows_in_quality,
    voltage_vs_quality_avg,
    last_kwh,
    voltage_percent_rank,
    first_voltage,
    kwh_vs_site_id_avg,
    voltage_vs_site_id_avg,
    quality_avg_voltage,
    kwh_sum_3,
    ROUND(voltage_percent_rank * 0.9, 2) AS voltage_percent_rank_adj,
    CONCAT(quality, '#', CAST(rows_in_quality AS STRING)) AS quality_ref
FROM metrics
ORDER BY quality_ref NULLS LAST
