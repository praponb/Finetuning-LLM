WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE kwh < 109.3
)
SELECT 'reading' AS src, quality, quality_len
FROM valid
WHERE TO_DATE(read_at) < DATE '2025-03-02'
  AND quality NOT IN ('OK', 'EST')
UNION ALL
SELECT 'tariff' AS src, tariff AS quality, id AS quality_len
FROM tariff_windows
WHERE rate <= 6.1
