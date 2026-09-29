WITH cleaned_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_TRUNC('WEEK', commissioned_on) AS commissioned_on_week,
        LENGTH(TRIM(tariff)) AS tariff_len,
        DATEDIFF(DATE '2025-12-31', commissioned_on) AS days_since_commissioned_on
    FROM sites
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_name,
        s.zone,
        s.commissioned_on_week,
        s.tariff_len,
        s.days_since_commissioned_on,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM cleaned_sites s
    JOIN meter_readings m ON m.site_id = s.id
),
with_parent_reading AS (
    -- Enrich with dimension data
    SELECT s.*, m.voltage AS reading_voltage_2
    FROM line_level_readings s
    JOIN meter_readings m ON m.id = s.reading_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN reading_quality IN ('OK', 'BAD') THEN 'group_a' WHEN reading_quality = 'EST' THEN 'group_b' ELSE 'other' END AS reading_quality_group,
        DATE_FORMAT(reading_read_at, 'yyyy-MM-dd') AS reading_read_at_label
    FROM with_parent_reading
    WHERE zone <> 'Z4'
)
-- Subtotals with GROUPING SETS
SELECT
    reading_quality_group,
    zone,
    reading_quality,
    MIN(reading_read_at) AS first_reading_read_at,
    COUNT(DISTINCT reading_id) AS distinct_reading_ids
FROM enriched
GROUP BY GROUPING SETS ((reading_quality_group, zone, reading_quality), (reading_quality_group), (zone))
ORDER BY reading_quality_group NULLS LAST, zone NULLS LAST, reading_quality NULLS LAST
