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
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM covered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IN ('BAD', 'EST')
              AND m.voltage < 238.3
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(voltage) AS max_reading_voltage,
        COUNT(*) FILTER (WHERE quality IS NOT NULL) AS reading_quality_count,
        MIN(voltage) AS min_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.max_reading_voltage,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.min_reading_voltage
    FROM without_matches s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        zone,
        rate,
        energy_cost,
        reading_quality_count,
        ROW_NUMBER() OVER (PARTITION BY reading_quality_count ORDER BY kwh DESC NULLS FIRST, id) AS rn,
        SUM(kwh) OVER (PARTITION BY reading_quality_count ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS kwh_sum_3_prior,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY zone)) AS distinct_site_id_in_zone
    FROM summarized_rows
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    zone,
    rate,
    energy_cost,
    reading_quality_count,
    rn,
    kwh_sum_3_prior,
    CONCAT(zone, '#', CAST(rn AS STRING)) AS zone_ref,
    HOUR(read_at) AS read_at_hour
FROM windowed
ORDER BY zone DESC NULLS FIRST, read_at
