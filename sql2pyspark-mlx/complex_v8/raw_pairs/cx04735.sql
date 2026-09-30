WITH prepared AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE rate > 3.0
      AND valid_to >= TIMESTAMP '2025-02-16 05:00:00'
)
SELECT
    tariff,
    rate,
    valid_from AS event_time,
    'tariff' AS record_type
FROM prepared
WHERE tariff <> 'FLAT'
  AND rate <= 6.1
UNION ALL
SELECT
    quality AS tariff,
    voltage AS rate,
    read_at AS event_time,
    'reading' AS record_type
FROM meter_readings
