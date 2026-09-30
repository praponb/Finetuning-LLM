WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label_2
    FROM meter_readings
    WHERE site_id >= 6
      AND read_at >= TIMESTAMP '2025-01-25 11:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        ROUND(AVG(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_4,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id), 2) AS site_id_avg_kwh,
        RANK() OVER w_all_by_voltage_ties AS voltage_rank
    FROM base_readings
    WINDOW w_all_by_voltage_ties AS (ORDER BY voltage)
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.read_at_label,
        s.read_at_label_2,
        s.voltage_avg_4,
        s.kwh_vs_site_id_avg,
        s.site_id_avg_kwh,
        s.voltage_rank,
        si.tariff AS site_tariff
    FROM with_windows s
    JOIN sites si ON si.id = s.site_id
)
SELECT *
FROM (
    SELECT site_id, site_tariff, quality, kwh_vs_site_id_avg
    FROM enriched
) src
PIVOT (
    SUM(kwh_vs_site_id_avg) AS total, COUNT(kwh_vs_site_id_avg) AS n
    FOR quality IN ('OK', 'BAD')
)
ORDER BY site_id, site_tariff
