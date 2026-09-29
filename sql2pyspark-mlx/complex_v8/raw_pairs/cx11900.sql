WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 109.3
      AND site_id IS NOT NULL
)
SELECT quality, kwh, 'reading' AS source
FROM cleaned_readings
WHERE quality IS NOT NULL
  AND voltage > 216.5
UNION ALL
SELECT tariff AS quality, rate AS kwh, 'tariff' AS source
FROM tariff_windows
UNION ALL
SELECT site_name AS quality, installed_kw AS kwh, 'site' AS source
FROM sites
WHERE zone IS NOT NULL
ORDER BY quality DESC, source
