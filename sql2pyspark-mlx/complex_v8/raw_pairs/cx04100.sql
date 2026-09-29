WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.days_since_read_at,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM filtered_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.days_since_read_at,
        s.reading_id,
        s.reading_read_at,
        s.reading_quality,
        si.commissioned_on AS site_commissioned_on,
        si.site_name,
        si.tariff AS site_tariff
    FROM detail_rows_readings s
    JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone, si.tariff AS site_tariff_2
    FROM with_details_site s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    site_id,
    reading_id,
    GROUPING(site_id) AS is_total_site_id,
    COUNT(*) FILTER (WHERE reading_quality IN ('EST', 'BAD')) AS n_reading_quality_in_est_bad,
    ROUND(100 * COUNT(CASE WHEN reading_quality NOT IN ('EST', 'BAD') THEN 1 END) / COUNT(*), 1) AS pct_reading_quality_not_in_est_b
FROM enriched
GROUP BY GROUPING SETS ((site_id, reading_id), (site_id), (reading_id))
ORDER BY site_id NULLS LAST, reading_id NULLS LAST
