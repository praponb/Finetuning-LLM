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
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.rate,
        s.energy_cost,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM covered s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    reading_id,
    site_id,
    reading_quality,
    GROUPING(reading_id) AS is_total_reading_id,
    ARRAY_SORT(COLLECT_SET(reading_quality)) AS reading_quality_list
FROM exploded_detail
GROUP BY GROUPING SETS ((reading_id, site_id, reading_quality), (reading_id), (site_id))
