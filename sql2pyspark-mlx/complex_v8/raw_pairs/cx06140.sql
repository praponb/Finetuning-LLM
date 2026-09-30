WITH covered AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM covered s
)
SELECT
    id,
    read_at,
    voltage,
    zone,
    rate,
    energy_cost,
    CASE WHEN zone IN ('Z2', 'Z3') THEN 'group_a' WHEN zone = 'Z4' THEN 'group_b' ELSE 'other' END AS zone_group
FROM with_stats
WHERE kwh < 90.6
ORDER BY zone DESC, energy_cost NULLS LAST
