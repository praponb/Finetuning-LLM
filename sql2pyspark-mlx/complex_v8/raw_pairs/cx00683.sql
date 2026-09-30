WITH cleaned_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATE_TRUNC('WEEK', read_at) AS read_at_week,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 14:00:00'
      AND kwh <= 83.8
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(voltage) AS min_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage
    FROM cleaned_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
deduped AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        read_at_week,
        voltage_filled,
        min_reading_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY voltage DESC, id DESC) AS recency_rank
        FROM profiled
    ) ranked
    WHERE recency_rank = 1
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.read_at_week,
        s.min_reading_voltage,
        si.tariff AS site_tariff
    FROM deduped s
    JOIN sites si ON si.id = s.site_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(read_at_week, 'yyyy-MM-dd HH:00') AS read_at_week_label,
        LOWER(TRIM(site_tariff)) AS site_tariff_clean
    FROM with_parent_site
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM annotated s
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    read_at_week,
    min_reading_voltage,
    site_tariff_clean,
    reading_count,
    CONCAT(site_tariff, '#', CAST(site_id AS STRING)) AS site_tariff_ref,
    reading_count - min_reading_voltage AS reading_count_minus_min_reading_voltage
FROM with_stats
ORDER BY reading_count, site_id DESC NULLS FIRST
