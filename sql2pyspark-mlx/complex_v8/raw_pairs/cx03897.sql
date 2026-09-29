WITH scoped AS (
    -- Scope the sites rows
    SELECT
        id,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE LOWER(site_name) LIKE '% e%'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        installed_kw,
        commissioned_on,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM scoped
    WHERE commissioned_on < DATE '2024-11-21'
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.kwh < 87.1
              AND m.voltage > 221.8
        )
      AND s.installed_kw < 386.6
)
SELECT
    id,
    installed_kw,
    DATE_TRUNC('WEEK', commissioned_on) AS commissioned_on_week,
    FLOOR(installed_kw_bucket / 100) * 100 AS installed_kw_bucket_bucket
FROM qualified
ORDER BY id
