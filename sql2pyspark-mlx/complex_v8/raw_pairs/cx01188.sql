WITH in_window AS (
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
latest AS (
    -- Keep the latest row per site_id
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
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS row_num
        FROM in_window
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        si.site_name,
        si.zone AS site_zone
    FROM latest s
    LEFT JOIN sites si ON si.id = s.site_id AND UPPER(TRIM(si.site_name)) LIKE 'M%'
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    rate,
    energy_cost,
    site_zone
FROM enriched
ORDER BY id DESC
