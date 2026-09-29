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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.zone,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM covered s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        reading_id,
        reading_kwh,
        reading_quality,
        ROW_NUMBER() OVER (PARTITION BY reading_id, zone ORDER BY reading_read_at ASC, id, reading_id) AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY reading_id, zone ORDER BY reading_read_at ASC, id, reading_id), 3) AS reading_read_at_cume_dist,
        LEAD(reading_voltage, 3) OVER (PARTITION BY reading_id, zone ORDER BY reading_read_at ASC, id, reading_id) AS next3_reading_voltage,
        NTH_VALUE(reading_kwh, 2) OVER (PARTITION BY reading_id, zone ORDER BY reading_read_at ASC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reading_kwh
    FROM with_children
),
combined_details AS (
    -- Join meter_readings
    SELECT
        s.id,
        s.site_id,
        s.reading_id,
        s.reading_quality,
        s.reading_read_at_cume_dist,
        s.next3_reading_voltage,
        m.site_id AS reading_site_id,
        m.voltage AS reading_voltage
    FROM with_windows s
    JOIN meter_readings m ON m.id = s.reading_id
)
SELECT
    COALESCE(reading_quality, 'ALL') AS reading_quality,
    reading_site_id,
    reading_id,
    GROUPING(reading_quality) AS is_total_reading_quality,
    COUNT(*) AS cnt,
    COUNT(DISTINCT reading_site_id) AS distinct_reading_site_ids,
    SUM(CASE WHEN reading_quality = 'EST' THEN 1 ELSE 0 END) AS n_reading_quality_est
FROM combined_details
GROUP BY GROUPING SETS ((reading_quality, reading_site_id, reading_id), (reading_quality))
ORDER BY reading_quality NULLS LAST, reading_site_id NULLS LAST, reading_id NULLS LAST
