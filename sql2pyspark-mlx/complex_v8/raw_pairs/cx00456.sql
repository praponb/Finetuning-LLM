WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM meter_readings
    WHERE (kwh <= 106.0 OR site_id <= 2)
),
enriched AS (
    -- Join sites
    SELECT
        s.*,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM valid_readings s
    LEFT JOIN sites si ON si.id = s.site_id AND si.tariff <> 'TOU'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_read_at_month AS rows_in_read_at_month,
        ROUND(AVG(kwh) OVER (PARTITION BY MONTH(read_at), site_id), 2) AS read_at_month_site_id_avg_kwh,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_read_at_month_site_id, 0), 4) AS voltage_share_of_read_at_month_site_id,
        SUM(least_kwh_voltage) OVER (PARTITION BY MONTH(read_at), site_id) AS read_at_month_site_id_sum_least_kwh_voltage,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY YEAR(read_at), site_tariff ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        NTILE(10) OVER (PARTITION BY site_tariff, quality ORDER BY kwh DESC, id) AS kwh_decile,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_tariff, quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY MONTH(read_at), site_id), 2) AS voltage_vs_read_at_month_site_id_avg,
        ROUND(CUME_DIST() OVER w_read_at_year_site_tariff_by_2, 3) AS kwh_cume_dist,
        COUNT(*) OVER (PARTITION BY MONTH(read_at), site_id) AS rows_in_read_at_month_site_id
    FROM enriched
    WINDOW w_read_at_month AS (PARTITION BY MONTH(read_at)), w_read_at_month_site_id AS (PARTITION BY MONTH(read_at), site_id), w_read_at_year_site_tariff_by_2 AS (PARTITION BY YEAR(read_at), site_tariff ORDER BY kwh, id)
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(kwh_decile - 5) AS kwh_decile_distance,
        DATE_FORMAT(second_read_at, 'yyyy-MM-dd') AS second_read_at_label,
        CONCAT_WS('-', site_zone, quality) AS site_zone_quality_label
    FROM with_windows
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    quality,
    quality_or_unknown,
    least_kwh_voltage,
    site_tariff,
    site_zone,
    rows_in_read_at_month,
    read_at_month_site_id_sum_least_kwh_voltage,
    second_read_at,
    kwh_decile,
    last_known_voltage,
    voltage_vs_read_at_month_site_id_avg,
    kwh_cume_dist,
    rows_in_read_at_month_site_id,
    kwh_decile_distance,
    site_zone_quality_label,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
FROM enriched_2
ORDER BY site_zone_quality_label, id
LIMIT 20
