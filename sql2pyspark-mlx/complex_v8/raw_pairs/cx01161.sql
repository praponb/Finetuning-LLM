WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_group,
        s.read_at_month,
        s.quality_len,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM cleaned s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with GROUPING SETS
SELECT
    quality_len,
    site_id,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM joined
GROUP BY GROUPING SETS ((quality_len, site_id), (quality_len), ())
