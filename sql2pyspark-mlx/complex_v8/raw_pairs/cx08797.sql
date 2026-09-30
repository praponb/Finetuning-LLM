WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-18'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM base_readings
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality_len,
        s.quality_slug,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM tagged s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff = 'FLAT'
),
annotated AS (
    -- Add calculated columns
    SELECT *, COALESCE(site_tariff, 'UNKNOWN') AS site_tariff_or_unknown
    FROM combined_details
)
SELECT *
FROM (
    SELECT site_commissioned_on, read_at, site_tariff_or_unknown, site_installed_kw
    FROM annotated
) src
PIVOT (
    MAX(site_installed_kw)
    FOR site_tariff_or_unknown IN ('TOU', 'UNKNOWN', 'FLAT')
)
ORDER BY site_commissioned_on, read_at
