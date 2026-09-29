WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        ADD_MONTHS(commissioned_on, 3) AS commissioned_on_plus_3m,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
    FROM sites
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.id) AS first_reading_read_at,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.id) AS max_reading_voltage
    FROM filtered s
),
zone_or_unknown_stats AS (
    -- Avg max_reading_voltage per zone_or_unknown
    SELECT zone_or_unknown, AVG(max_reading_voltage) AS avg_max_reading_voltage
    FROM summarized_rows
    GROUP BY zone_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_max_reading_voltage,
        ROUND(s.max_reading_voltage - g.avg_max_reading_voltage, 2) AS max_reading_voltage_minus_avg
    FROM summarized_rows s
    JOIN zone_or_unknown_stats g ON g.zone_or_unknown = s.zone_or_unknown
    WHERE s.max_reading_voltage > g.avg_max_reading_voltage
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        installed_kw - max_reading_voltage AS installed_kw_minus_max_reading_voltage,
        GREATEST(avg_max_reading_voltage, max_reading_voltage) AS greatest_avg_max_reading_voltage_max_reading_vol,
        LOWER(REGEXP_REPLACE(TRIM(zone_or_unknown), ' ', '_')) AS zone_or_unknown_slug
    FROM outliers
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(commissioned_on, 'yyyy-MM-dd') AS commissioned_on_label,
        COALESCE(avg_max_reading_voltage, 0.0) AS avg_max_reading_voltage_filled
    FROM annotated
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(CASE WHEN quality = 'BAD' THEN 1 END) AS reading_quality_count,
        MIN(read_at) AS first_reading_read_at_2,
        ROUND(AVG(voltage), 2) AS avg_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.first_reading_read_at_2,
        m.avg_reading_voltage
    FROM scored s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('|', zone, commissioned_on_label) AS zone_commissioned_on_label_label,
        INITCAP(TRIM(zone_or_unknown)) AS zone_or_unknown_clean
    FROM with_stats
)
SELECT
    id,
    site_name,
    zone,
    tariff,
    commissioned_on,
    commissioned_on_plus_3m,
    zone_or_unknown,
    max_reading_voltage,
    max_reading_voltage_minus_avg,
    installed_kw_minus_max_reading_voltage,
    greatest_avg_max_reading_voltage_max_reading_vol,
    zone_or_unknown_slug,
    commissioned_on_label,
    avg_max_reading_voltage_filled,
    reading_quality_count,
    first_reading_read_at_2,
    avg_reading_voltage,
    zone_commissioned_on_label_label,
    zone_or_unknown_clean,
    LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug,
    LENGTH(TRIM(zone_or_unknown)) AS zone_or_unknown_len
FROM labeled
WHERE avg_reading_voltage BETWEEN 220.5 AND 232.8
