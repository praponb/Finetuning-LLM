WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
rollup_base AS (
    -- Group-level totals
    SELECT
        site_id,
        TO_DATE(read_at) AS read_at_day,
        COUNT(*) AS row_count,
        ROUND(AVG(voltage), 2) AS avg_voltage,
        MAX(read_at) AS last_read_at,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM prepared_readings
    GROUP BY 1, 2
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.site_id,
        s.read_at_day,
        s.row_count,
        s.avg_voltage,
        s.last_read_at,
        s.distinct_site_ids,
        s.median_kwh,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM rollup_base s
    JOIN sites si ON si.id = s.site_id
),
unique_rows AS (
    -- Keep the latest row per site_zone
    SELECT
        site_id,
        read_at_day,
        row_count,
        avg_voltage,
        last_read_at,
        distinct_site_ids,
        median_kwh,
        site_zone,
        site_commissioned_on,
        site_installed_kw
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_zone ORDER BY median_kwh DESC, site_id DESC, read_at_day DESC) AS rn
        FROM enriched_site
    ) ranked
    WHERE rn = 1
)
SELECT *
FROM (
    SELECT distinct_site_ids, row_count, site_zone, site_installed_kw
    FROM unique_rows
) src
PIVOT (
    COUNT(site_installed_kw)
    FOR site_zone IN ('Z4', 'Z3', 'Z2')
)
