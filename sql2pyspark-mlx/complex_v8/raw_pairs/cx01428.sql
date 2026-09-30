WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE site_id <= 7
      AND voltage >= 219.5
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM valid_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(kwh), 2) AS avg_reading_kwh,
        MIN(read_at) AS first_reading_read_at,
        COUNT(CASE WHEN quality = 'OK' THEN 1 END) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_kwh,
        m.first_reading_read_at,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM detail_rows s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        quality,
        reading_id,
        reading_read_at,
        reading_voltage,
        reading_quality,
        avg_reading_kwh,
        first_reading_read_at,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CONCAT(reading_quality, '#', CAST(id AS STRING)) AS reading_quality_ref,
        COALESCE(reading_quality, 'UNKNOWN') AS reading_quality_or_unknown
    FROM with_stats
    WHERE reading_kwh <= 109.0
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(avg_reading_kwh / 25) * 25 AS avg_reading_kwh_bucket,
        INITCAP(TRIM(reading_quality_ref)) AS reading_quality_ref_clean
    FROM derived
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.reading_id,
        s.reading_voltage,
        s.reading_quality,
        s.avg_reading_kwh,
        s.first_reading_read_at,
        s.quality_slug,
        s.avg_reading_kwh_bucket,
        s.reading_quality_ref_clean,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on
    FROM tagged s
    JOIN sites si ON si.id = s.site_id
),
without_matches AS (
    -- Keep rows whose reading matches
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.id = s.reading_id
              AND m.voltage BETWEEN 213.7 AND 241.9
        )
)
SELECT
    id,
    site_id,
    kwh,
    quality,
    reading_id,
    reading_voltage,
    reading_quality,
    avg_reading_kwh,
    avg_reading_kwh_bucket,
    reading_quality_ref_clean,
    site_tariff,
    site_commissioned_on,
    ROUND(reading_voltage * 1.1, 2) AS reading_voltage_adj
FROM without_matches
ORDER BY site_tariff DESC, reading_voltage DESC
