WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ABS(kwh - 83.8) AS kwh_distance,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_ref,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM recent_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        COUNT(*) AS reading_count,
        MAX(read_at) AS last_reading_read_at_2
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.last_reading_read_at_2
    FROM with_children_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.site_name,
        m.read_at AS reading_read_at_2
    FROM with_counts s
    LEFT JOIN sites si ON si.id = s.site_id
    LEFT JOIN meter_readings m ON m.id = s.reading_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, si.tariff AS site_tariff
    FROM combined_details s
    LEFT JOIN sites si ON si.id = s.site_id AND si.tariff IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(voltage) OVER (PARTITION BY site_zone, reading_count ORDER BY last_reading_read_at, id, reading_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_max_1_prior,
        DENSE_RANK() OVER (PARTITION BY reading_id ORDER BY kwh DESC, id, reading_id) AS kwh_dense_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_zone, site_tariff), 0), 4) AS voltage_share_of_site_zone_site_tariff,
        SUM(reading_count) OVER (PARTITION BY site_zone, reading_count ORDER BY last_reading_read_at, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_reading_count,
        SIZE(COLLECT_SET(reading_id) OVER (PARTITION BY site_zone, site_tariff)) AS distinct_reading_id_in_site_zone_site_tariff
    FROM with_details
)
SELECT
    id,
    site_id,
    kwh,
    quality_ref,
    reading_id,
    reading_kwh,
    reading_read_at,
    last_reading_read_at,
    last_reading_read_at_2,
    site_zone,
    site_commissioned_on,
    site_name,
    reading_read_at_2,
    site_tariff,
    voltage_max_1_prior,
    kwh_dense_rank,
    voltage_share_of_site_zone_site_tariff,
    running_reading_count,
    distinct_reading_id_in_site_zone_site_tariff,
    CONCAT(site_zone, '#', CAST(kwh_dense_rank AS STRING)) AS site_zone_ref
FROM ranked
ORDER BY kwh, last_reading_read_at, id, reading_id
LIMIT 20
