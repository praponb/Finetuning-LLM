WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.voltage) AS prior_voltage_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.prior_rows_1d,
        s.prior_voltage_1d,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM covered s
    LEFT JOIN sites si ON si.id = s.site_id
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.prior_rows_1d,
        s.prior_voltage_1d,
        s.site_tariff,
        s.site_zone,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM with_details_site s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    prior_rows_1d,
    prior_voltage_1d,
    site_zone,
    reading_id,
    reading_kwh,
    reading_read_at
FROM exploded_detail
WHERE site_zone = 'Z4'
ORDER BY id DESC NULLS FIRST, prior_rows_1d, reading_id
LIMIT 20
