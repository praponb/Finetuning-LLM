WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage > 222.3
      AND site_id IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.commissioned_on AS site_commissioned_on
    FROM prepared s
    LEFT JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        quality,
        LEAD(voltage) OVER w_all_by_kwh AS next_voltage,
        RANK() OVER w_all_by_site_commissioned_on AS site_commissioned_on_rank,
        LAG(kwh, 3) OVER w_quality_site_id_by_kwh AS prev3_kwh,
        RANK() OVER w_quality_by_voltage_ties AS voltage_rank,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        voltage - LAG(voltage) OVER w_quality_by_voltage AS voltage_change,
        DATEDIFF(site_commissioned_on, LAG(site_commissioned_on) OVER (ORDER BY kwh ASC NULLS LAST, id)) AS days_since_prev_site_commissioned_on,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        FIRST_VALUE(voltage) OVER (ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id, quality), 0), 4) AS kwh_share_of_site_id_quality,
        ROUND(AVG(voltage) OVER (PARTITION BY quality ORDER BY voltage DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        ROUND(voltage - AVG(voltage) OVER w_quality_site_id, 2) AS voltage_vs_quality_site_id_avg
    FROM with_details
    WINDOW w_all_by_kwh AS (ORDER BY kwh ASC NULLS LAST, id), w_all_by_site_commissioned_on AS (ORDER BY site_commissioned_on DESC), w_quality_site_id_by_kwh AS (PARTITION BY quality, site_id ORDER BY kwh ASC, id), w_quality_by_voltage_ties AS (PARTITION BY quality ORDER BY voltage DESC), w_site_id AS (PARTITION BY site_id), w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage DESC, id), w_quality_site_id AS (PARTITION BY quality, site_id)
),
tagged AS (
    -- Derive helper columns
    SELECT *, FLOOR(next_voltage / 5) * 5 AS next_voltage_bucket
    FROM metrics
    WHERE kwh_share_of_site_id_quality <= 0.8
)
SELECT
    id,
    next_voltage,
    prev3_kwh,
    voltage_rank,
    kwh_vs_site_id_avg,
    voltage_change,
    first_voltage,
    kwh_share_of_site_id_quality,
    voltage_vs_quality_site_id_avg,
    LEAST(kwh_vs_site_id_avg, kwh) AS least_kwh_vs_site_id_avg_kwh
FROM tagged
WHERE voltage_vs_quality_site_id_avg <= 147.0
