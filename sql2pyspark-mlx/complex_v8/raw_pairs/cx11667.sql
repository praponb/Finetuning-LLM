WITH in_window AS (
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
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        kwh,
        quality,
        zone,
        rate,
        energy_cost,
        LAG(energy_cost) OVER (ORDER BY energy_cost DESC, id) AS prev_energy_cost,
        COUNT(*) OVER w_zone_quality AS rows_in_zone_quality,
        ROUND(rate - AVG(rate) OVER (PARTITION BY zone, quality), 2) AS rate_vs_zone_quality_avg,
        DENSE_RANK() OVER (ORDER BY energy_cost DESC, id) AS energy_cost_dense_rank,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY energy_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(energy_cost - AVG(energy_cost) OVER (PARTITION BY zone, quality), 2) AS energy_cost_vs_zone_quality_avg
    FROM in_window
    WINDOW w_zone_quality AS (PARTITION BY zone, quality)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, rows_in_zone_quality, energy_cost_dense_rank
    FROM sequenced
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (rows_in_zone_quality, energy_cost_dense_rank)
) unp
ORDER BY id, measure
