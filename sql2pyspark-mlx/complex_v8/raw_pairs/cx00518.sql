WITH recent_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        commissioned_on,
        COALESCE(installed_kw, 0.0) AS installed_kw_filled,
        CASE WHEN installed_kw >= 373.3 THEN 'HIGH' WHEN installed_kw >= 96.9 THEN 'MEDIUM' ELSE 'LOW' END AS installed_kw_tier
    FROM sites
    WHERE installed_kw < 446.9
      AND commissioned_on IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(installed_kw_filled / 100) * 100 AS installed_kw_filled_bucket,
        LENGTH(TRIM(site_name)) AS site_name_len
    FROM recent_sites
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.tariff,
        s.installed_kw,
        s.commissioned_on,
        s.installed_kw_filled,
        s.installed_kw_tier,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM enriched s
    JOIN meter_readings m ON m.site_id = s.id
),
with_details_reading AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.commissioned_on,
        s.installed_kw_filled,
        s.installed_kw_tier,
        s.reading_id,
        s.reading_quality,
        s.reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality_2,
        m.site_id AS reading_site_id
    FROM detail_rows_readings s
    JOIN meter_readings m ON m.id = s.reading_id
    WHERE m.kwh < 83.8
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, reading_id, metric, value
FROM (
    SELECT id, reading_id, installed_kw_filled, reading_kwh
    FROM with_details_reading
) src
UNPIVOT (
    value FOR metric IN (installed_kw_filled, reading_kwh)
) unp
