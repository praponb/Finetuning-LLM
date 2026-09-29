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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
deduped AS (
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
            ROW_NUMBER() OVER (PARTITION BY site_id, zone ORDER BY voltage DESC, id DESC) AS recency_rank
        FROM covered
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    zone,
    rate,
    energy_cost
FROM deduped
ORDER BY site_id NULLS LAST, voltage
