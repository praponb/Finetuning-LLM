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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        zone,
        energy_cost,
        rate - LAG(rate) OVER (PARTITION BY zone, site_id ORDER BY voltage NULLS LAST, id) AS rate_change,
        ROUND(rate - AVG(rate) OVER (PARTITION BY site_id), 2) AS rate_vs_site_id_avg,
        LAG(rate) OVER (PARTITION BY zone, site_id ORDER BY voltage NULLS LAST, id) AS prev_rate
    FROM matched
)
SELECT
    id,
    site_id,
    zone,
    energy_cost,
    rate_change,
    rate_vs_site_id_avg,
    rate_change - energy_cost AS rate_change_minus_energy_cost
FROM metrics
WHERE energy_cost BETWEEN 135.0 AND 630.0
ORDER BY rate_change
