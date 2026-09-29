WITH joined_ranges AS (
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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
unique_rows AS (
    -- Keep the latest row per site_id, zone
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, zone ORDER BY rate DESC, id DESC) AS row_num
        FROM joined_ranges
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    zone,
    rate,
    energy_cost,
    COALESCE(voltage, 0.0) AS voltage_filled
FROM unique_rows
ORDER BY id NULLS LAST
