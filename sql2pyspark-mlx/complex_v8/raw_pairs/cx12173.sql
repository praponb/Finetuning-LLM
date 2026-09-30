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
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        energy_cost,
        SUM(voltage) OVER (PARTITION BY site_id, TO_DATE(read_at)) AS site_id_read_at_to_date_sum_voltage,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at ASC NULLS LAST), 3) AS read_at_percent_rank
    FROM covered
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IS NOT NULL
        )
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.energy_cost,
        s.site_id_read_at_to_date_sum_voltage,
        s.read_at_percent_rank,
        si.tariff AS site_tariff,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM screened s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    voltage,
    energy_cost,
    read_at_percent_rank,
    site_name,
    site_commissioned_on
FROM joined
WHERE read_at_percent_rank < 0.7
ORDER BY voltage NULLS LAST
