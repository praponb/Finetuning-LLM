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
scored AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(rate, energy_cost) AS least_rate_energy_cost,
        LOWER(TRIM(quality)) AS quality_clean
    FROM covered
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, kwh, rate, energy_cost, least_rate_energy_cost
    FROM scored
) src
UNPIVOT (
    value FOR measure IN (kwh AS kwh_v, rate AS m_rate, energy_cost AS energy_cost_v, least_rate_energy_cost AS m_least_rate_energy_cost)
) unp
ORDER BY id, measure
