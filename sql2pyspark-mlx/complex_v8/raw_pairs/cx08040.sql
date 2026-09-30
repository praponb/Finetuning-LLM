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
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(energy_cost) OVER (PARTITION BY zone, site_id), 2) AS zone_site_id_avg_energy_cost,
        voltage - LAG(voltage) OVER (PARTITION BY zone ORDER BY voltage, id) AS voltage_change,
        SUM(energy_cost) OVER (PARTITION BY zone, TO_DATE(read_at) ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS energy_cost_sum_7,
        FIRST_VALUE(rate) OVER (PARTITION BY zone ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rate,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY zone, site_id), 0), 4) AS voltage_share_of_zone_site_id
    FROM matched
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(rate / NULLIF(energy_cost_sum_7, 0), 3) AS rate_per_energy_cost_sum_7,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        CASE WHEN voltage_change >= 73.5 THEN 'A' WHEN voltage_change >= -73.5 THEN 'B' ELSE 'C' END AS voltage_change_tier
    FROM windowed
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    zone,
    energy_cost,
    zone_site_id_avg_energy_cost,
    voltage_change,
    energy_cost_sum_7,
    rate_per_energy_cost_sum_7,
    days_since_read_at,
    COALESCE(kwh, 0.0) AS kwh_filled
FROM labeled
ORDER BY voltage, read_at DESC
