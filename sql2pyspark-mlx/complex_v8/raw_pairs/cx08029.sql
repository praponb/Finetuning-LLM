WITH covered AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage
    FROM covered s
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.zone,
        s.rate,
        s.energy_cost,
        s.max_reading_voltage,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM with_counts s
    JOIN sites si ON si.id = s.site_id
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        read_at + INTERVAL 2 HOURS AS read_at_plus_2h,
        SUBSTRING(TRIM(zone), 1, 4) AS zone_prefix,
        ABS(rate - 5.2) AS rate_distance
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, rate, rate_distance, energy_cost
    FROM enriched_2
) src
UNPIVOT (
    amount FOR field IN (rate, rate_distance, energy_cost)
) unp
