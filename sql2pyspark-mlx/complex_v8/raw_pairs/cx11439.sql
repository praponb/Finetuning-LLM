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
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM joined_ranges s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND HOUR(m.read_at) BETWEEN 8 AND 14
        )
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        zone,
        rate,
        energy_cost,
        LEAD(kwh) OVER (PARTITION BY zone, quality ORDER BY kwh DESC, id) AS next_kwh,
        ROUND(AVG(kwh) OVER (PARTITION BY zone, quality ORDER BY kwh DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_6_prior,
        MAX(rate) OVER (PARTITION BY zone, quality ORDER BY kwh DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS rate_max_1_prior,
        MAX(energy_cost) OVER (PARTITION BY site_id) AS site_id_max_energy_cost,
        LAST_VALUE(rate) OVER (PARTITION BY zone, quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rate,
        SUM(kwh) OVER (PARTITION BY zone, site_id ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_sum_2_prior
    FROM screened
),
screened_2 AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone IN ('Z4', 'Z2')
        )
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    zone,
    rate,
    next_kwh,
    kwh_avg_6_prior,
    rate_max_1_prior,
    last_rate,
    kwh_sum_2_prior,
    YEAR(read_at) AS read_at_year
FROM screened_2
WHERE site_id <= 3
ORDER BY read_at, next_kwh DESC NULLS FIRST
