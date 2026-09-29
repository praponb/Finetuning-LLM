WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        read_at + INTERVAL 30 MINUTES AS read_at_plus_30m,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix_2
    FROM meter_readings
    WHERE kwh > 45.0
),
latest AS (
    -- Keep the latest row per site_id, quality
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_prefix,
        read_at_plus_30m,
        quality_prefix_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY read_at DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-03-08 09:00:00'
              AND m.voltage BETWEEN 220.7 AND 236.8
        )
),
with_parent AS (
    -- Join sites
    SELECT s.*, si.site_name, si.zone AS site_zone
    FROM with_activity s
    JOIN sites si ON si.id = s.site_id AND si.tariff = 'TOU'
)
SELECT
    COALESCE(site_zone, 'ALL') AS site_zone,
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    GROUPING(site_zone) AS is_total_site_zone,
    MIN(voltage) AS min_voltage,
    COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null
FROM with_parent
GROUP BY ROLLUP(site_zone, quality, site_id)
ORDER BY site_zone NULLS LAST, quality NULLS LAST, site_id NULLS LAST
