WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE kwh <= 86.0
      AND voltage BETWEEN 221.4 AND 239.6
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        si.zone AS site_zone
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.site_zone,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM joined s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage >= 224.9
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    site_zone,
    reading_id,
    ABS(kwh - 53.6) AS kwh_distance
FROM line_level_readings
WHERE voltage > 221.8
ORDER BY id DESC
