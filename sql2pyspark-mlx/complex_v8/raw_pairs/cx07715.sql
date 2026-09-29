WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE site_id <= 5
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.tariff AS site_tariff, si.zone AS site_zone
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
),
with_parent AS (
    -- Join sites
    SELECT
        s.*,
        si.zone AS site_zone_2,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM with_details_site s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM with_parent s
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        quality,
        site_tariff,
        site_zone_2,
        site_commissioned_on,
        site_name,
        first_reading_read_at,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_zone_2, DATE_TRUNC('MONTH', read_at) ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        avg_reading_voltage - LAG(avg_reading_voltage) OVER (ORDER BY avg_reading_voltage ASC, id) AS avg_reading_voltage_change,
        ROUND(AVG(avg_reading_voltage) OVER (ORDER BY avg_reading_voltage ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS avg_reading_voltage_avg_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY site_zone_2, DATE_TRUNC('MONTH', read_at) ORDER BY kwh NULLS LAST), 3) AS kwh_cume_dist,
        ROUND(kwh - AVG(kwh) OVER w_first_reading_read_at_month, 2) AS kwh_vs_first_reading_read_at_month_site_zone_avg
    FROM with_counts
    WINDOW w_first_reading_read_at_month AS (PARTITION BY DATE_TRUNC('MONTH', first_reading_read_at), site_zone)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    site_tariff,
    site_zone_2,
    site_name,
    first_reading_read_at,
    last_known_kwh,
    avg_reading_voltage_avg_2_prior,
    kwh_vs_first_reading_read_at_month_site_zone_avg,
    read_at + INTERVAL 7 DAYS AS read_at_plus_7d,
    SUBSTRING(TRIM(site_zone_2), 1, 3) AS site_zone_2_prefix
FROM with_windows
ORDER BY last_known_kwh NULLS LAST, site_id NULLS LAST
