WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        TRUNC(commissioned_on, 'MM') AS commissioned_on_month_start
    FROM sites
    WHERE tariff IN ('EV', 'FLAT')
      AND installed_kw <= 373.3
)
SELECT id, installed_kw AS value
FROM base
WHERE commissioned_on_month_start BETWEEN DATE '2024-05-01' AND DATE '2025-10-01'
  AND commissioned_on < DATE '2024-11-21'
UNION ALL
SELECT id, installed_kw AS value
FROM sites
UNION ALL
SELECT site_id AS id, voltage AS value
FROM meter_readings
WHERE quality = 'BAD'
ORDER BY value
