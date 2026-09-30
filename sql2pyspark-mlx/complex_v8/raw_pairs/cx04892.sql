WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.kwh >= 15.9
              AND MONTH(m.read_at) IN (2, 3)
        )
      AND s.tariff NOT IN ('EV', 'TOU')
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.tariff,
        s.installed_kw,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM without_matches s
    LEFT JOIN meter_readings m ON m.site_id = s.id
),
joined_reading AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.tariff,
        s.reading_id,
        s.reading_read_at,
        s.reading_quality,
        m.read_at AS reading_read_at_2
    FROM with_children_readings s
    LEFT JOIN meter_readings m ON m.id = s.reading_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        tariff,
        reading_id,
        reading_read_at,
        reading_quality,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        SUBSTRING(TRIM(reading_quality), 1, 2) AS reading_quality_prefix
    FROM joined_reading
    WHERE reading_id IS NOT NULL
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', tariff_or_unknown, tariff) AS tariff_or_unknown_tariff_label,
        QUARTER(reading_read_at) AS reading_read_at_quarter,
        SUBSTRING(TRIM(reading_quality), 1, 3) AS reading_quality_prefix_2
    FROM enriched
),
enriched_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.tariff,
        s.reading_id,
        s.reading_read_at,
        s.reading_quality,
        s.tariff_or_unknown,
        s.reading_quality_prefix,
        s.reading_read_at_quarter,
        s.reading_quality_prefix_2,
        m.site_id AS reading_site_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality_2
    FROM labeled s
    LEFT JOIN meter_readings m ON m.id = s.reading_id
)
SELECT
    id,
    tariff,
    reading_id,
    reading_read_at,
    reading_read_at_quarter,
    reading_quality_prefix_2,
    reading_site_id,
    reading_kwh,
    reading_quality_2,
    CONCAT_WS(' / ', reading_quality_2, reading_quality_prefix_2) AS reading_quality_2_reading_quality_prefix_2_label,
    DATEDIFF(DATE '2025-12-31', reading_read_at) AS days_since_reading_read_at
FROM enriched_2
