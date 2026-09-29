WITH combined AS (
    -- Stack the branches
    SELECT
        id AS entity_id,
        voltage,
        quality,
        'reading' AS source
    FROM meter_readings
    UNION ALL
    SELECT
        id,
        installed_kw,
        tariff,
        'site' AS source
    FROM sites
    WHERE LENGTH(TRIM(site_name)) >= 5
      AND zone = 'Z2'
)
SELECT source, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM combined
GROUP BY source
ORDER BY source
