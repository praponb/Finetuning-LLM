WITH matched AS (
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
enriched AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        rate,
        energy_cost,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter,
        GREATEST(energy_cost, voltage) AS greatest_energy_cost_voltage
    FROM matched
)
-- Final projection
SELECT
    id,
    rate,
    energy_cost,
    read_at_quarter,
    greatest_energy_cost_voltage,
    ROUND(rate * 1.07, 2) AS rate_adj
FROM enriched
ORDER BY energy_cost DESC NULLS FIRST, rate_adj DESC NULLS FIRST
