WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_TRUNC('WEEK', commissioned_on) AS commissioned_on_week
    FROM sites
    WHERE commissioned_on IS NOT NULL
)
SELECT id AS key_id
FROM base
EXCEPT
SELECT site_id
FROM meter_readings
ORDER BY key_id
