WITH joined_ranges AS (
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
grouped AS (
    -- Group-level totals
    SELECT
        zone,
        quality,
        MAX(read_at) AS last_read_at,
        MAX(energy_cost) AS max_energy_cost,
        ROUND(AVG(rate), 2) AS avg_rate,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(energy_cost), 2) AS total_energy_cost
    FROM joined_ranges
    GROUP BY 1, 2
)
-- Final projection
SELECT
    zone,
    quality,
    last_read_at,
    max_energy_cost,
    avg_rate,
    cnt,
    distinct_ids,
    total_energy_cost,
    total_energy_cost + avg_rate AS total_energy_cost_plus_avg_rate
FROM grouped
ORDER BY cnt NULLS LAST, max_energy_cost
