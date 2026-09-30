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
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality <> 'EST'
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY energy_cost, id), 3) AS energy_cost_cume_dist,
        NTH_VALUE(rate, 2) OVER (ORDER BY energy_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rate,
        COUNT(*) OVER (PARTITION BY MONTH(read_at)) AS rows_in_read_at_month,
        ROW_NUMBER() OVER (ORDER BY energy_cost, id) AS rn,
        LAST_VALUE(rate) IGNORE NULLS OVER (ORDER BY energy_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rate
    FROM qualified
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    zone,
    rate,
    energy_cost,
    energy_cost_cume_dist,
    last_known_rate,
    ABS(energy_cost_cume_dist - 0.5) AS energy_cost_cume_dist_distance,
    DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
FROM with_trends
ORDER BY read_at DESC NULLS FIRST, energy_cost DESC
