WITH scoped AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug,
        LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug
    FROM sites
    WHERE zone IN ('Z4', 'Z2')
      AND UPPER(TRIM(site_name)) LIKE 'H%'
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.kwh < 106.5
              AND m.read_at < TIMESTAMP '2025-02-18 07:00:00'
        )
)
SELECT
    id,
    zone,
    commissioned_on,
    COALESCE(zone, 'UNKNOWN') AS zone_or_unknown,
    FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
FROM without_matches
