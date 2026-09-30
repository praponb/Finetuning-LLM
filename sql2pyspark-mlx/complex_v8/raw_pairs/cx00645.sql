WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage > 219.6
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality_slug,
        si.site_name,
        si.tariff AS site_tariff
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id AND si.tariff = 'TOU'
    WHERE si.tariff IN ('TOU', 'EV')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        quality_slug,
        site_tariff,
        SUM(voltage) OVER w_site_id AS site_id_sum_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, site_tariff ORDER BY voltage), 3) AS voltage_cume_dist,
        ROUND(CUME_DIST() OVER w_all_by_voltage_ties, 3) AS voltage_cume_dist_2,
        ROW_NUMBER() OVER w_site_tariff_by_voltage AS voltage_seq,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage,
        ROUND(CUME_DIST() OVER (ORDER BY voltage, id), 3) AS voltage_cume_dist_3,
        LEAD(voltage, 2) OVER (PARTITION BY site_tariff ORDER BY voltage DESC, id) AS next2_voltage,
        ROUND(AVG(voltage) OVER (PARTITION BY site_tariff, site_id ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        SUM(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage
    FROM combined_details
    WINDOW w_site_id AS (PARTITION BY site_id), w_all_by_voltage_ties AS (ORDER BY voltage), w_site_tariff_by_voltage AS (PARTITION BY site_tariff ORDER BY voltage DESC, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        site_id_sum_voltage + voltage_cume_dist_2 AS site_id_sum_voltage_plus_voltage_cume_dist_2,
        CASE WHEN rows_in_site_id >= 15 THEN 'HIGH' WHEN rows_in_site_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END AS rows_in_site_id_tier,
        COALESCE(site_tariff, 'UNKNOWN') AS site_tariff_or_unknown
    FROM with_windows
    WHERE site_id_sum_voltage BETWEEN 770.0 AND 2002.0
)
SELECT
    id,
    quality_slug,
    site_tariff,
    site_id_sum_voltage,
    voltage_cume_dist,
    voltage_cume_dist_2,
    site_id_avg_voltage,
    voltage_cume_dist_3,
    next2_voltage,
    voltage_avg_3,
    rows_in_site_id,
    site_id_sum_voltage_plus_voltage_cume_dist_2,
    rows_in_site_id_tier,
    site_tariff_or_unknown,
    COALESCE(site_id_sum_voltage, 0.0) AS site_id_sum_voltage_filled,
    COALESCE(next2_voltage, 0.0) AS next2_voltage_filled
FROM scored
WHERE site_id_avg_voltage <= 234.5
ORDER BY quality_slug, site_id_sum_voltage_plus_voltage_cume_dist_2 DESC
